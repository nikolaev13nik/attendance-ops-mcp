import asyncio
from typing import Annotated, Any, Literal

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from attendance_ops_mcp.config import OWN_PACKAGE, ChosenEnv, EnvParam, Target, target_for
from attendance_ops_mcp.kube import find_pod, kubectl, list_pods, service_of
from attendance_ops_mcp.problems import LEVEL_RANK, Group, Problem, group_problems, problem_from_line
from attendance_ops_mcp.server import mcp

MAX_SINCE_MINUTES = 30 * 24 * 60
MAX_SCAN_LINES_PER_POD = 20000
MAX_GROUPS = 20


def restart_count(pod: dict[str, Any]) -> int:
    return sum(status.get("restartCount", 0) for status in pod["status"].get("containerStatuses", []))


async def read_pod_lines(target: Target, pod: dict[str, Any], since_minutes: int) -> list[tuple[bool, str]]:
    args = ["-n", target.namespace, "logs", pod["metadata"]["name"], f"--since={since_minutes}m", f"--tail={MAX_SCAN_LINES_PER_POD}"]
    lines = [(False, line) for line in (await kubectl(target, *args, timeout=60)).splitlines()]
    if restart_count(pod) > 0:
        try:
            previous = await kubectl(target, *args, "--previous", timeout=60)
        except ToolError:
            previous = ""
        lines += [(True, line) for line in previous.splitlines()]
    return lines


def short_time(timestamp: str) -> str:
    return timestamp[5:19].replace("T", " ") if timestamp else "?"


def render_group(number: int, group: Group) -> list[str]:
    problem: Problem = group.example
    when = f"first {short_time(group.first)}  last {short_time(group.last)}" if group.first else ""
    restart = "  (before last restart)" if group.before_restart else ""
    lines = [f"[{number}] {group.service} ×{group.count}  {when}{restart}".rstrip(), f"    {problem.logger}: {problem.message}"]
    if problem.chain:
        lines.append(f"    chain: {' → '.join(problem.chain)}")
        lines.append(f"    root cause: {problem.root_cause}")
        if problem.via:
            lines.append(f"    via: {problem.via}")
    if problem.own_frames:
        lines.append(f"    your code: {' ← '.join(problem.own_frames)}")
    if problem.context:
        lines.append(f"    example: {problem.context}")
    return lines


@mcp.tool(
    description=(
        "Find and summarise errors in the services' logs without reading whole logs: scans the "
        "current and pre-restart logs of every pod (or one service), groups identical problems, "
        "and for each group shows the count, first/last time, the exception chain, the root cause "
        "(deepest 'Caused by'), the first frames in the project's own code and an example "
        "tenant/trace. Use level='WARN' to include warnings. Zoom in afterwards with pod_logs + grep."
    ),
    annotations=ToolAnnotations(readOnlyHint=True),
)
async def find_exceptions(
    choice: ChosenEnv,
    env: EnvParam = None,
    service: Annotated[str | None, Field(description="Only this service, e.g. accounting. Default: all services")] = None,
    since_minutes: Annotated[int, Field(description="Look back this many minutes")] = 60,
    level: Annotated[Literal["ERROR", "WARN"], Field(description="Lowest level to include")] = "ERROR",
    max_groups: Annotated[int, Field(description=f"How many groups to show, 1-{MAX_GROUPS}")] = 10,
) -> str:
    target = target_for(choice.env)
    since_minutes = min(max(since_minutes, 1), MAX_SINCE_MINUTES)
    max_groups = min(max(max_groups, 1), MAX_GROUPS)
    if service is not None:
        await find_pod(target, service)
    pods = await list_pods(target, "-l", f"app={service}") if service is not None else await list_pods(target)
    per_pod = await asyncio.gather(*(read_pod_lines(target, pod, since_minutes) for pod in pods))
    found = []
    scanned = 0
    for pod, lines in zip(pods, per_pod):
        scanned += len(lines)
        for before_restart, line in lines:
            problem = problem_from_line(line, OWN_PACKAGE, LEVEL_RANK[level])
            if problem is not None:
                found.append((service_of(pod), before_restart, problem))
    groups = group_problems(found)
    header = f"{target.label} {level} and above, last {since_minutes} min, scanned {len(pods)} pod(s) / {scanned} line(s)"
    if len(groups) == 0:
        return f"{header}\nNo problems found."
    lines = [f"{header}\n{len(groups)} distinct problem(s), {len(found)} occurrence(s)"]
    for number, group in enumerate(groups[:max_groups], start=1):
        lines += ["", *render_group(number, group)]
    if len(groups) > max_groups:
        lines += ["", f"... {len(groups) - max_groups} more group(s) not shown"]
    lines += ["", "Zoom in with pod_logs(service, grep=...)."]
    return "\n".join(lines)
