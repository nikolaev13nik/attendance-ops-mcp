import re
from typing import Annotated, Any

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from attendance_ops_mcp.config import ChosenEnv, EnvParam, Target, target_for
from attendance_ops_mcp.kube import find_pod, kubectl, kubectl_json, list_pods, service_of
from attendance_ops_mcp.logformat import compact_line
from attendance_ops_mcp.server import mcp
from attendance_ops_mcp.tables import format_table
from attendance_ops_mcp.timeutil import age, minutes_since, parse_time

MAX_LOG_LINES = 500
GREP_SCAN_LINES = 5000
MAX_LINE_LENGTH = 400
MAX_EVENT_GROUPS = 20
RECENT_RESTART_MINUTES = 60
READ_ONLY = ToolAnnotations(readOnlyHint=True)


def cpu_cores(value: str) -> float:
    if value.endswith("m"):
        return int(value[:-1]) / 1000
    return float(value)


def is_node_ready(node: dict[str, Any]) -> bool:
    return any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"])


def pod_state(pod: dict[str, Any]) -> str:
    for status in pod["status"].get("containerStatuses", []):
        state = status.get("state", {})
        if "waiting" in state:
            return state["waiting"].get("reason", "Waiting")
        if "terminated" in state:
            return state["terminated"].get("reason", "Terminated")
    return pod["status"].get("phase", "Unknown")


def ready_count(pod: dict[str, Any]) -> tuple[int, int]:
    statuses = pod["status"].get("containerStatuses", [])
    return sum(1 for s in statuses if s.get("ready")), len(pod["spec"]["containers"])


def last_restart(pod: dict[str, Any]) -> tuple[int, str | None, str | None]:
    restarts, finished_at, reason = 0, None, None
    for status in pod["status"].get("containerStatuses", []):
        restarts += status.get("restartCount", 0)
        terminated = status.get("lastState", {}).get("terminated")
        if terminated is not None:
            finished_at, reason = terminated.get("finishedAt"), terminated.get("reason")
    return restarts, finished_at, reason


def image_name(pod: dict[str, Any]) -> str:
    return pod["spec"]["containers"][0]["image"].split("/")[-1]


def describe_pod(pod: dict[str, Any]) -> tuple[list[str], list[str]]:
    ready, total = ready_count(pod)
    restarts, finished_at, reason = last_restart(pod)
    restart_text = str(restarts)
    if restarts > 0 and finished_at is not None:
        restart_text = f"{restarts} ({age(finished_at)} ago, {reason})"
    row = [service_of(pod), pod_state(pod), f"{ready}/{total}", restart_text, age(pod["metadata"]["creationTimestamp"]), image_name(pod)]
    warnings = []
    if ready < total:
        warnings.append(f"{service_of(pod)} is not ready ({pod_state(pod)})")
    restarted_at = parse_time(finished_at)
    if restarted_at is not None and minutes_since(restarted_at) <= RECENT_RESTART_MINUTES:
        warnings.append(f"{service_of(pod)} restarted {age(finished_at)} ago ({reason})")
    return row, warnings


async def cpu_requests_text(target: Target, nodes: list[dict[str, Any]]) -> str:
    try:
        all_pods = await kubectl_json(target, "get", "pods", "-A", "--field-selector", "status.phase=Running")
    except ToolError:
        return ""
    allocatable = sum(cpu_cores(node["status"]["allocatable"]["cpu"]) for node in nodes if is_node_ready(node))
    if allocatable == 0:
        return ""
    requested = sum(
        cpu_cores(container.get("resources", {}).get("requests", {}).get("cpu", "0"))
        for pod in all_pods["items"]
        for container in pod["spec"]["containers"]
    )
    return f" · cpu requests {requested / allocatable:.0%} of allocatable"


@mcp.tool(
    description=(
        "Health of the Attendance cluster in one call: ready nodes, CPU requests, and one line per "
        "service with status, readiness, restarts (with when and why), age and image version. "
        "Ends with warnings for pods that are not ready or restarted in the last hour."
    ),
    annotations=READ_ONLY,
)
async def cluster_status(choice: ChosenEnv, env: EnvParam = None) -> str:
    target = target_for(choice.env)
    nodes = (await kubectl_json(target, "get", "nodes"))["items"]
    ready_nodes = [node for node in nodes if is_node_ready(node)]
    if len(ready_nodes) == 0 and target.is_gke:
        return f"{target.label} cluster is stopped (0 ready nodes). The owner starts it with scripts/gcp-start.sh."
    pods = sorted(await list_pods(target), key=service_of)
    described = [describe_pod(pod) for pod in pods]
    all_ready = sum(1 for pod in pods if ready_count(pod)[0] == ready_count(pod)[1])
    lines = [
        f"{target.label} cluster status",
        f"nodes: {len(ready_nodes)}/{len(nodes)} ready{await cpu_requests_text(target, nodes)}",
        f"pods: {all_ready}/{len(pods)} ready",
        "",
        *format_table(["SERVICE", "STATUS", "READY", "RESTARTS", "AGE", "IMAGE"], [row for row, _ in described]),
    ]
    warnings = [warning for _, pod_warnings in described for warning in pod_warnings]
    if warnings:
        lines += ["", "warnings:", *(f"⚠️  {warning}" for warning in warnings)]
    return "\n".join(lines)


@mcp.tool(
    description=(
        f"Read recent log lines of one service's pod, newest last. At most {MAX_LOG_LINES} lines are "
        "returned. JSON log lines are shown compactly as 'time level logger message [tenant user trace]', "
        "with the first line of any stack trace after ↳. Use grep to filter (case-insensitive regex) before lines reach you, since_minutes "
        "to limit the time window, and previous=true for the log from before the last crash. "
        "For errors, prefer find_exceptions, which summarises stack traces."
    ),
    annotations=READ_ONLY,
)
async def pod_logs(
    choice: ChosenEnv,
    service: Annotated[str, Field(description="Short service name, e.g. accounting, timetracking, aiassistance, gui, kafka, postgres")],
    env: EnvParam = None,
    lines: Annotated[int, Field(description=f"How many lines to return, 1-{MAX_LOG_LINES}")] = 200,
    since_minutes: Annotated[int | None, Field(description="Only lines from the last N minutes")] = None,
    grep: Annotated[str | None, Field(description="Case-insensitive regex; only matching lines are returned")] = None,
    previous: Annotated[bool, Field(description="Log of the previous container, before its last restart")] = False,
) -> str:
    target = target_for(choice.env)
    lines = min(max(lines, 1), MAX_LOG_LINES)
    pattern = None
    if grep is not None:
        try:
            pattern = re.compile(grep, re.IGNORECASE)
        except re.error as error:
            raise ToolError(f"grep is not a valid regex: {error}") from error
    pod = await find_pod(target, service)
    args = ["-n", target.namespace, "logs", pod, f"--tail={lines if pattern is None else GREP_SCAN_LINES}"]
    if since_minutes is not None:
        args.append(f"--since={max(since_minutes, 1)}m")
    if previous:
        args.append("--previous")
    log_lines = (await kubectl(target, *args, timeout=60)).splitlines()
    if pattern is not None:
        log_lines = [line for line in log_lines if pattern.search(line)]
    selected = [compact_line(line)[:MAX_LINE_LENGTH] for line in log_lines[-lines:]]
    window = f", last {since_minutes} min" if since_minutes is not None else ""
    match = f", matching /{grep}/" if grep is not None else ""
    which = " (previous container)" if previous else ""
    header = f"{target.label} {pod}{which}: {len(selected)} line(s){window}{match}"
    return "\n".join([header, *selected]) if selected else f"{header}\n(no lines)"


def event_time(event: dict[str, Any]) -> str | None:
    series = event.get("series") or {}
    return series.get("lastObservedTime") or event.get("lastTimestamp") or event.get("eventTime") or event.get("firstTimestamp")


def event_count(event: dict[str, Any]) -> int:
    series = event.get("series") or {}
    return series.get("count") or event.get("count") or 1


@mcp.tool(
    description=(
        "Kubernetes warning events in the Attendance namespace from the last N minutes, grouped by "
        "object and reason with counts: OOMKilled, BackOff, failed probes, failed image pulls, "
        "scheduling problems. Often the real reason a pod is down."
    ),
    annotations=READ_ONLY,
)
async def recent_events(
    choice: ChosenEnv,
    env: EnvParam = None,
    minutes: Annotated[int, Field(description="Look back this many minutes")] = 30,
) -> str:
    target = target_for(choice.env)
    events = (await kubectl_json(target, "-n", target.namespace, "get", "events", "--field-selector", "type=Warning"))["items"]
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        when = parse_time(event_time(event))
        if when is None or minutes_since(when) > minutes:
            continue
        involved = event["involvedObject"]
        key = (f"{involved.get('kind', '?')}/{involved.get('name', '?')}", event.get("reason", "?"))
        group = groups.setdefault(key, {"count": 0, "last": when, "message": ""})
        group["count"] += event_count(event)
        if when >= group["last"]:
            group["last"], group["message"] = when, event.get("message", "")
    if len(groups) == 0:
        return f"{target.label} no warning events in the last {minutes} min."
    ordered = sorted(groups.items(), key=lambda item: item[1]["last"], reverse=True)[:MAX_EVENT_GROUPS]
    lines = [f"{target.label} warning events, last {minutes} min ({len(groups)} group(s))"]
    for (obj, reason), group in ordered:
        last = age(group["last"].isoformat())
        message = group["message"].replace("\n", " ")[:MAX_LINE_LENGTH]
        lines.append(f"⚠️  {reason} ×{group['count']} on {obj}, last {last} ago: {message}")
    return "\n".join(lines)
