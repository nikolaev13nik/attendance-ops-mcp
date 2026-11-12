import base64
from dataclasses import dataclass
from typing import Any

import psycopg
from mcp.server.mcpserver.exceptions import ToolError
from psycopg import sql

from attendance_ops_mcp.config import (
    POSTGRES_DATABASE,
    POSTGRES_PASSWORD_KEY,
    POSTGRES_SERVICE,
    POSTGRES_USER,
    SECRET_NAME,
    Target,
)
from attendance_ops_mcp.kube import kubectl
from attendance_ops_mcp.tunnel import get_tunnel

POSTGRES_PORT = 5432
STATEMENT_TIMEOUT_MS = 5000
CONNECT_TIMEOUT_SECONDS = 10
SESSION_OPTIONS = f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c default_transaction_read_only=on"

passwords: dict[str, str] = {}

Query = str | sql.Composable


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    truncated: bool


async def postgres_password(target: Target) -> str:
    if target.context in passwords:
        return passwords[target.context]
    encoded = await kubectl(
        target, "-n", target.namespace, "get", "secret", SECRET_NAME, "-o", f"jsonpath={{.data.{POSTGRES_PASSWORD_KEY}}}"
    )
    if encoded.strip() == "":
        raise ToolError(f"{target.label} secret {SECRET_NAME} has no key {POSTGRES_PASSWORD_KEY}.")
    passwords[target.context] = base64.b64decode(encoded).decode()
    return passwords[target.context]


def first_line(error: Exception) -> str:
    text = str(error).strip()
    return text.splitlines()[0] if text else type(error).__name__


async def connect(target: Target) -> psycopg.AsyncConnection[Any]:
    tunnel = get_tunnel(target, POSTGRES_SERVICE, POSTGRES_PORT)
    password = await postgres_password(target)
    for attempt in (1, 2):
        port = await tunnel.open()
        try:
            connection = await psycopg.AsyncConnection.connect(
                host="127.0.0.1",
                port=port,
                dbname=POSTGRES_DATABASE,
                user=POSTGRES_USER,
                password=password,
                connect_timeout=CONNECT_TIMEOUT_SECONDS,
                options=SESSION_OPTIONS,
                application_name="attendance-ops-mcp",
            )
        except psycopg.OperationalError as error:
            await tunnel.close()
            if attempt == 2:
                raise ToolError(f"{target.label} cannot connect to Postgres: {first_line(error)}") from error
            continue
        await connection.set_read_only(True)
        return connection
    raise ToolError(f"{target.label} cannot connect to Postgres.")


async def run_query(target: Target, query: Query, params: dict[str, Any] | None = None, limit: int = 1000) -> QueryResult:
    connection = await connect(target)
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(query, params, prepare=True)
            if cursor.description is None:
                return QueryResult([], [], False)
            rows = await cursor.fetchmany(limit + 1)
            columns = [column.name for column in cursor.description]
            return QueryResult(columns, rows[:limit], len(rows) > limit)
    except psycopg.Error as error:
        raise ToolError(f"{target.label} Postgres error: {first_line(error)}") from error
    finally:
        await connection.rollback()
        await connection.close()
