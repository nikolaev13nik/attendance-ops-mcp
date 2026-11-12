import asyncio
import json
import re
import shutil
import subprocess
from typing import Any

from mcp.server.mcpserver.exceptions import ToolError

from attendance_ops_mcp.config import Target

DEFAULT_TIMEOUT_SECONDS = 30
SERVICE_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")


async def run_process(command: list[str], timeout: float = DEFAULT_TIMEOUT_SECONDS) -> subprocess.CompletedProcess[str]:
    if shutil.which(command[0]) is None:
        raise ToolError(f"'{command[0]}' is not installed or not on PATH. Call check_setup.")
    try:
        return await asyncio.to_thread(
            subprocess.run, command, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as error:
        raise ToolError(f"'{' '.join(command)}' timed out after {timeout:.0f}s.") from error


def last_line(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def explain_failure(stderr: str, target: Target) -> str:
    text = stderr.lower()
    if "context" in text and "does not exist" in text:
        return f"The kube context '{target.context}' is missing. Call check_setup."
    if "connection refused" in text or "unable to connect" in text or "i/o timeout" in text:
        if target.is_gke:
            return "The GKE API is unreachable. Call check_setup."
        return "The cluster is unreachable. Is Rancher Desktop running?"
    if "forbidden" in text:
        return "Permission denied. Call check_setup to see which permissions are missing."
    if "gke-gcloud-auth-plugin" in text or "gcloud" in text:
        return "GKE login failed. Call check_setup."
    return ""


async def kubectl(target: Target, *args: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> str:
    result = await run_process(["kubectl", "--context", target.context, *args], timeout)
    if result.returncode != 0:
        reason = last_line(result.stderr)
        hint = explain_failure(result.stderr, target)
        raise ToolError(f"{target.label} kubectl {' '.join(args)} failed: {reason} {hint}".strip())
    return result.stdout


async def kubectl_json(target: Target, *args: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Any:
    return json.loads(await kubectl(target, *args, "-o", "json", timeout=timeout))


async def list_pods(target: Target, *selector: str) -> list[dict[str, Any]]:
    result = await kubectl_json(target, "-n", target.namespace, "get", "pods", *selector)
    return result["items"]


def service_of(pod: dict[str, Any]) -> str:
    return pod["metadata"].get("labels", {}).get("app", pod["metadata"]["name"])


async def find_pod(target: Target, service: str) -> str:
    if SERVICE_PATTERN.fullmatch(service) is None:
        raise ToolError(f"'{service}' is not a valid service name. Use a short name like 'accounting'.")
    pods = await list_pods(target, "-l", f"app={service}")
    if len(pods) == 0:
        services = sorted({service_of(pod) for pod in await list_pods(target)})
        raise ToolError(f"{target.label} no pod for service '{service}'. Known services: {', '.join(services)}")
    running = [pod for pod in pods if pod["status"].get("phase") == "Running"]
    newest = max(running or pods, key=lambda pod: pod["metadata"]["creationTimestamp"])
    return newest["metadata"]["name"]
