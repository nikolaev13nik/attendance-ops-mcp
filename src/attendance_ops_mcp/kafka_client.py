import asyncio
import logging
from collections.abc import Callable

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError
from mcp.server.mcpserver.exceptions import ToolError

from attendance_ops_mcp.config import KAFKA_EXTERNAL_PORT, KAFKA_SERVICE, Target
from attendance_ops_mcp.tunnel import Tunnel, close_other_contexts, get_tunnel

CLIENT_ID = "attendance-ops-mcp"
REQUEST_TIMEOUT_MS = 15000

logging.getLogger("kafka").setLevel(logging.WARNING)


async def kafka_bootstrap(tunnel: Tunnel) -> str:
    try:
        port = await tunnel.open()
    except ToolError as error:
        if "address already in use" in str(error):
            raise ToolError(
                f"{error} Local port {KAFKA_EXTERNAL_PORT} is taken by another program "
                "(a Docker Compose Kafka?). Kafka advertises localhost:"
                f"{KAFKA_EXTERNAL_PORT}, so the tunnel must use exactly that port."
            ) from error
        raise
    return f"localhost:{port}"


async def with_kafka[T](target: Target, work: Callable[[str], T]) -> T:
    await close_other_contexts(KAFKA_SERVICE, KAFKA_EXTERNAL_PORT, target.context)
    tunnel = get_tunnel(target, KAFKA_SERVICE, KAFKA_EXTERNAL_PORT, local_port=KAFKA_EXTERNAL_PORT)
    async with tunnel.lease():
        bootstrap = await kafka_bootstrap(tunnel)
        try:
            return await asyncio.to_thread(work, bootstrap)
        except KafkaError as error:
            raise ToolError(f"{target.label} Kafka error: {error}") from error


def new_admin(bootstrap: str) -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=bootstrap, client_id=CLIENT_ID, request_timeout_ms=REQUEST_TIMEOUT_MS)


def new_consumer(bootstrap: str) -> KafkaConsumer:
    return KafkaConsumer(
        bootstrap_servers=bootstrap,
        client_id=CLIENT_ID,
        group_id=None,
        enable_auto_commit=False,
        allow_auto_create_topics=False,
        request_timeout_ms=REQUEST_TIMEOUT_MS,
    )
