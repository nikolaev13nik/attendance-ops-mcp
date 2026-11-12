import json
import shutil
from dataclasses import dataclass
from typing import Literal

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from attendance_ops_mcp.config import ChosenEnv, EnvParam, Target, target_for
from attendance_ops_mcp.kube import last_line, run_process
from attendance_ops_mcp.server import mcp

Status = Literal["ok", "fail", "warn"]
ICONS: dict[Status, str] = {"ok": "✅", "fail": "❌", "warn": "⚠️ "}

REQUIRED_PERMISSIONS = [
    ("list", "pods", "list pods"),
    ("get", "pods/log", "read pod logs"),
    ("create", "pods/portforward", "open tunnels to Postgres and Kafka"),
    ("get", "secrets", "read the Postgres password secret"),
]


@dataclass
class Check:
    status: Status
    text: str
    fix: str = ""

    def render(self) -> str:
        line = f"{ICONS[self.status]} {self.text}"
        return f"{line}\n      → {self.fix}" if self.fix else line


def get_credentials_command(context: str) -> str:
    _, project, zone, cluster = context.split("_", 3)
    return f"gcloud container clusters get-credentials {cluster} --zone {zone} --project {project}"


async def check_kubectl() -> Check:
    if shutil.which("kubectl") is None:
        return Check("fail", "kubectl not found", "Install kubectl: https://kubernetes.io/docs/tasks/tools/")
    result = await run_process(["kubectl", "version", "--client", "-o", "json"])
    version = json.loads(result.stdout)["clientVersion"]["gitVersion"]
    return Check("ok", f"kubectl {version}")


async def check_gcloud() -> list[Check]:
    if shutil.which("gcloud") is None:
        return [Check("fail", "gcloud not found", "Install the Google Cloud CLI: https://cloud.google.com/sdk/docs/install")]
    checks = []
    result = await run_process(["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"])
    account = last_line(result.stdout)
    if account:
        checks.append(Check("ok", f"gcloud logged in as {account}"))
    else:
        checks.append(Check("fail", "gcloud is not logged in", "gcloud auth login"))
    if shutil.which("gke-gcloud-auth-plugin") is None:
        checks.append(Check("fail", "gke-gcloud-auth-plugin not found", "gcloud components install gke-gcloud-auth-plugin"))
    else:
        checks.append(Check("ok", "gke-gcloud-auth-plugin installed"))
    return checks


async def check_context(target: Target) -> Check:
    result = await run_process(["kubectl", "config", "get-contexts", "-o", "name"])
    if target.context in result.stdout.split():
        return Check("ok", f"kube context {target.context}")
    if target.is_gke:
        return Check("fail", f"kube context {target.context} missing", get_credentials_command(target.context))
    return Check("fail", f"kube context {target.context} missing", "Install Rancher Desktop and enable Kubernetes")


async def check_api(target: Target) -> Check:
    result = await run_process(["kubectl", "--context", target.context, "version", "-o", "json"], timeout=20)
    server = json.loads(result.stdout or "{}").get("serverVersion")
    if server:
        return Check("ok", f"cluster API reachable (Kubernetes {server['gitVersion']})")
    if target.is_gke:
        return Check("fail", f"GKE API unreachable: {last_line(result.stderr)}", "Check your network and gcloud login")
    return Check("fail", "cluster unreachable", "Start Rancher Desktop and wait until Kubernetes is running")


async def check_namespace(target: Target) -> Check:
    result = await run_process(["kubectl", "--context", target.context, "get", "namespace", target.namespace])
    if result.returncode == 0:
        return Check("ok", f"namespace {target.namespace} exists")
    return Check("fail", f"namespace {target.namespace} not found: {last_line(result.stderr)}", "Install the Attendance Helm charts into this cluster")


async def check_permissions(target: Target) -> list[Check]:
    checks = []
    for verb, resource, purpose in REQUIRED_PERMISSIONS:
        result = await run_process(
            ["kubectl", "--context", target.context, "auth", "can-i", verb, resource, "-n", target.namespace]
        )
        if result.stdout.strip() == "yes":
            checks.append(Check("ok", f"allowed to {purpose}"))
        else:
            checks.append(Check("fail", f"not allowed to {purpose} ({verb} {resource})", "Ask the project owner for access"))
    return checks


async def check_nodes(target: Target) -> Check:
    result = await run_process(["kubectl", "--context", target.context, "get", "nodes", "-o", "json"])
    if result.returncode != 0:
        return Check("warn", "cannot list nodes, skipping the running-node check")
    nodes = json.loads(result.stdout)["items"]
    ready = [
        node for node in nodes
        if any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"])
    ]
    if ready:
        return Check("ok", f"{len(ready)} node(s) ready")
    if target.is_gke:
        return Check("warn", "cluster is stopped (0 nodes)", "The owner starts it with scripts/gcp-start.sh (the node is billed while running)")
    return Check("warn", "no ready nodes")


async def run_checks(target: Target) -> list[Check]:
    checks = [await check_kubectl()]
    if target.is_gke:
        checks += await check_gcloud()
    if any(check.status == "fail" for check in checks):
        return checks
    checks.append(await check_context(target))
    if checks[-1].status == "fail":
        return checks
    checks.append(await check_api(target))
    if checks[-1].status == "fail":
        return checks
    checks.append(await check_namespace(target))
    checks += await check_permissions(target)
    checks.append(await check_nodes(target))
    return checks


def render_report(target: Target, checks: list[Check]) -> str:
    failures = sum(1 for check in checks if check.status == "fail")
    summary = "Ready to use." if failures == 0 else f"{failures} problem(s) to fix, top to bottom."
    lines = [f"{target.label} setup check", *(check.render() for check in checks), "", summary]
    return "\n".join(lines)


@mcp.tool(
    description=(
        "Check that this machine can use the attendance-ops tools against an environment: kubectl, "
        "gcloud login, kube context, cluster reachability, namespace, permissions, running nodes. "
        "Each problem comes with the command that fixes it. Call it first when another tool fails."
    ),
    annotations=ToolAnnotations(readOnlyHint=True),
)
async def check_setup(choice: ChosenEnv, env: EnvParam = None) -> str:
    target = target_for(choice.env)
    try:
        checks = await run_checks(target)
    except ToolError as error:
        checks = [Check("fail", str(error))]
    return render_report(target, checks)
