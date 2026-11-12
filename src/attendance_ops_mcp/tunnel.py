import asyncio
import atexit
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from mcp.server.mcpserver.exceptions import ToolError

from attendance_ops_mcp.config import Target
from attendance_ops_mcp.kube import last_line

FORWARDING = re.compile(r"Forwarding from 127\.0\.0\.1:(\d+)")
START_TIMEOUT_SECONDS = 20
STOP_TIMEOUT_SECONDS = 5
IDLE_CLOSE_SECONDS = float(os.environ.get("ATTENDANCE_TUNNEL_IDLE_SECONDS", "60"))


class Tunnel:
    def __init__(self, target: Target, resource: str, remote_port: int, local_port: int = 0) -> None:
        self.target = target
        self.resource = resource
        self.remote_port = remote_port
        self.local_port = local_port
        self.process: asyncio.subprocess.Process | None = None
        self.port: int | None = None
        self.drain: asyncio.Task[None] | None = None
        self.users = 0
        self.idle_close: asyncio.Task[None] | None = None
        self.lock = asyncio.Lock()

    @property
    def is_alive(self) -> bool:
        return self.process is not None and self.process.returncode is None

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[None]:
        self.users += 1
        self.cancel_idle_close()
        try:
            yield
        finally:
            self.users -= 1
            if self.users == 0 and self.is_alive:
                self.idle_close = asyncio.create_task(self.close_when_idle())

    def cancel_idle_close(self) -> None:
        if self.idle_close is not None:
            self.idle_close.cancel()
            self.idle_close = None

    async def close_when_idle(self) -> None:
        await asyncio.sleep(IDLE_CLOSE_SECONDS)
        self.idle_close = None
        if self.users == 0:
            await self.close()

    async def open(self) -> int:
        async with self.lock:
            return await self.start()

    async def start(self) -> int:
        if self.is_alive and self.port is not None:
            return self.port
        await self.stop()
        self.process = await asyncio.create_subprocess_exec(
            "kubectl", "--context", self.target.context, "-n", self.target.namespace,
            "port-forward", self.resource, f"{self.local_port}:{self.remote_port}",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        atexit.register(kill_quietly, self.process)
        try:
            self.port = await asyncio.wait_for(self.read_port(), START_TIMEOUT_SECONDS)
        except (TimeoutError, ToolError) as error:
            await self.stop()
            raise ToolError(
                f"{self.target.label} could not open a tunnel to {self.resource}:{self.remote_port}: {error}"
            ) from error
        self.drain = asyncio.create_task(self.discard_output())
        return self.port

    async def read_port(self) -> int:
        assert self.process is not None and self.process.stdout is not None and self.process.stderr is not None
        while True:
            line = await self.process.stdout.readline()
            if line == b"":
                stderr = (await self.process.stderr.read()).decode(errors="replace")
                raise ToolError(last_line(stderr) or "kubectl port-forward exited")
            match = FORWARDING.search(line.decode(errors="replace"))
            if match is not None:
                return int(match.group(1))

    async def discard_output(self) -> None:
        assert self.process is not None and self.process.stdout is not None and self.process.stderr is not None
        await asyncio.gather(discard(self.process.stdout), discard(self.process.stderr))

    async def close(self) -> None:
        self.cancel_idle_close()
        async with self.lock:
            await self.stop()

    async def stop(self) -> None:
        if self.drain is not None:
            self.drain.cancel()
        if self.is_alive:
            assert self.process is not None
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), STOP_TIMEOUT_SECONDS)
            except TimeoutError:
                self.process.kill()
        self.process, self.port, self.drain = None, None, None


async def discard(stream: asyncio.StreamReader) -> None:
    while await stream.read(65536):
        pass


def kill_quietly(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass


tunnels: dict[tuple[str, str, int], Tunnel] = {}


async def close_other_contexts(resource: str, remote_port: int, keep_context: str) -> None:
    for (context, other_resource, other_port), tunnel in tunnels.items():
        if other_resource == resource and other_port == remote_port and context != keep_context:
            await tunnel.close()


def get_tunnel(target: Target, resource: str, remote_port: int, local_port: int = 0) -> Tunnel:
    key = (target.context, resource, remote_port)
    if key not in tunnels:
        tunnels[key] = Tunnel(target, resource, remote_port, local_port)
    return tunnels[key]
