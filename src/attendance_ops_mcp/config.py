import os
from dataclasses import dataclass
from typing import Annotated, Literal

from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import Elicit, Resolve
from pydantic import BaseModel, Field

Env = Literal["dev", "prod"]

DEFAULT_CONTEXTS: dict[str, str] = {
    "dev": "rancher-desktop",
    "prod": "gke_attendance-512211_me-west1-a_attendance",
}
DEFAULT_NAMESPACE = "attendance"


@dataclass(frozen=True)
class Target:
    env: Env
    context: str
    namespace: str

    @property
    def is_gke(self) -> bool:
        return self.context.startswith("gke_")

    @property
    def label(self) -> str:
        platform = "GKE" if self.is_gke else self.context
        return f"[{self.env.upper()} · {platform}]"


def target_for(env: Env) -> Target:
    return Target(
        env=env,
        context=os.environ.get(f"{env.upper()}_CONTEXT", DEFAULT_CONTEXTS[env]),
        namespace=os.environ.get("ATTENDANCE_NAMESPACE", DEFAULT_NAMESPACE),
    )


class EnvChoice(BaseModel):
    env: Env


def choose_env(ctx: Context, env: Env | None = None) -> EnvChoice | Elicit[EnvChoice]:
    if env is not None:
        return EnvChoice(env=env)
    capabilities = ctx.client_capabilities
    if capabilities is None or capabilities.elicitation is None:
        raise ToolError("No environment given. Ask the user whether to use dev or prod, then call again with env set.")
    return Elicit("Which environment: dev (local Rancher Desktop) or prod (live GKE)?", EnvChoice)


ChosenEnv = Annotated[EnvChoice, Resolve(choose_env)]

EnvParam = Annotated[
    Env | None,
    Field(description="'dev' or 'prod'. Leave it out unless the user explicitly named the environment."),
]
