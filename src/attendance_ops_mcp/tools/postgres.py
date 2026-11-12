import re
from typing import Annotated

from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from psycopg import sql as pgsql
from pydantic import Field

from attendance_ops_mcp.config import ChosenEnv, EnvParam, Target, target_for
from attendance_ops_mcp.postgres import STATEMENT_TIMEOUT_MS, QueryResult, run_query
from attendance_ops_mcp.server import mcp
from attendance_ops_mcp.tables import cell, format_table

READ_ONLY = ToolAnnotations(readOnlyHint=True)
MAX_QUERY_ROWS = 500
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]{0,62}")
RECENT_MIGRATIONS = 3

TABLES_SQL = """
SELECT n.nspname AS schema, c.relname AS table, c.reltuples::bigint AS approx_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg\\_%%'
  AND (%(schema)s::text IS NULL OR n.nspname = %(schema)s)
ORDER BY 1, 2
"""

FIND_TABLE_SQL = """
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = %(table)s AND c.relkind IN ('r', 'p', 'v', 'm')
  AND (%(schema)s::text IS NULL OR n.nspname = %(schema)s)
ORDER BY 1
"""

COLUMNS_SQL = """
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = %(schema)s AND table_name = %(table)s
ORDER BY ordinal_position
"""

CONSTRAINTS_SQL = """
SELECT con.conname, pg_get_constraintdef(con.oid)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = %(schema)s AND c.relname = %(table)s
ORDER BY con.contype, con.conname
"""

INDEXES_SQL = """
SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname = %(schema)s AND tablename = %(table)s
ORDER BY indexname
"""

FLYWAY_SCHEMAS_SQL = """
SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'flyway_schema_history' AND c.relkind = 'r'
ORDER BY 1
"""


def render_result(target: Target, title: str, result: QueryResult) -> str:
    if len(result.columns) == 0:
        return f"{target.label} {title}: statement returned no rows."
    more = f" (more rows exist; showing the first {len(result.rows)})" if result.truncated else ""
    rows = [[cell(value) for value in row] for row in result.rows]
    return "\n".join([f"{target.label} {title}: {len(rows)} row(s){more}", *format_table(result.columns, rows)])


def check_identifier(value: str, what: str) -> str:
    if IDENTIFIER.fullmatch(value) is None:
        raise ToolError(f"'{value}' is not a valid {what} name.")
    return value


async def resolve_table(target: Target, name: str) -> tuple[str, str]:
    schema, _, table = name.rpartition(".")
    params = {"schema": check_identifier(schema, "schema") if schema else None, "table": check_identifier(table, "table")}
    schemas = [row[0] for row in (await run_query(target, FIND_TABLE_SQL, params)).rows]
    if len(schemas) == 0:
        raise ToolError(f"{target.label} table '{name}' not found. Call list_tables to see what exists.")
    if len(schemas) > 1:
        raise ToolError(f"{target.label} '{table}' exists in several schemas: {', '.join(schemas)}. Use schema.table.")
    return schemas[0], table


@mcp.tool(
    description=(
        "List the tables in the Attendance database with schema, approximate row count and size. "
        "Each service owns a schema (accounting, timetracking)."
    ),
    annotations=READ_ONLY,
)
async def list_tables(
    choice: ChosenEnv,
    env: EnvParam = None,
    schema: Annotated[str | None, Field(description="Only this schema, e.g. accounting")] = None,
) -> str:
    target = target_for(choice.env)
    params = {"schema": check_identifier(schema, "schema") if schema is not None else None}
    result = await run_query(target, TABLES_SQL, params)
    result.rows = [(s, t, "?" if rows < 0 else rows, size) for s, t, rows, size in result.rows]
    return render_result(target, "tables", result)


@mcp.tool(
    description="Describe one table: columns with types, nullability and defaults, constraints (primary and foreign keys, unique, checks) and indexes.",
    annotations=READ_ONLY,
)
async def describe_table(
    choice: ChosenEnv,
    table: Annotated[str, Field(description="Table as schema.table (e.g. accounting.tenant) or just the table name")],
    env: EnvParam = None,
) -> str:
    target = target_for(choice.env)
    schema, name = await resolve_table(target, table)
    params = {"schema": schema, "table": name}
    columns = await run_query(target, COLUMNS_SQL, params)
    constraints = await run_query(target, CONSTRAINTS_SQL, params)
    indexes = await run_query(target, INDEXES_SQL, params)
    lines = [
        f"{target.label} {schema}.{name}",
        *format_table(["COLUMN", "TYPE", "NULL", "DEFAULT"], [[cell(v) for v in row] for row in columns.rows]),
    ]
    if constraints.rows:
        lines += ["", "constraints:", *(f"  {n}: {d}" for n, d in constraints.rows)]
    if indexes.rows:
        lines += ["", "indexes:", *(f"  {d}" for _, d in indexes.rows)]
    return "\n".join(lines)


@mcp.tool(
    description=(
        "Run one read-only SQL statement (SELECT, WITH, EXPLAIN, SHOW) against the Attendance "
        "database and return the rows. Exactly one statement; it runs in a READ ONLY transaction "
        f"with a {STATEMENT_TIMEOUT_MS // 1000}s timeout, and writes are rejected by Postgres. "
        "Use schema-qualified names (accounting.x, timetracking.y); call list_tables/describe_table first."
    ),
    annotations=READ_ONLY,
)
async def query(
    choice: ChosenEnv,
    sql: Annotated[str, Field(description="One SQL statement")],
    env: EnvParam = None,
    limit: Annotated[int, Field(description=f"Max rows to return, 1-{MAX_QUERY_ROWS}")] = 100,
) -> str:
    target = target_for(choice.env)
    result = await run_query(target, sql, limit=min(max(limit, 1), MAX_QUERY_ROWS))
    return render_result(target, "query", result)


@mcp.tool(
    description=(
        "Flyway migration status for every schema that has a flyway_schema_history table: number of "
        "migrations, failed ones, and the most recent versions with when they were applied."
    ),
    annotations=READ_ONLY,
)
async def migration_status(choice: ChosenEnv, env: EnvParam = None) -> str:
    target = target_for(choice.env)
    schemas = [row[0] for row in (await run_query(target, FLYWAY_SCHEMAS_SQL)).rows]
    if len(schemas) == 0:
        return f"{target.label} no flyway_schema_history table found."
    lines = [f"{target.label} Flyway migrations"]
    for schema in schemas:
        history = pgsql.Identifier(schema, "flyway_schema_history")
        totals = await run_query(
            target, pgsql.SQL("SELECT count(*), count(*) FILTER (WHERE NOT success) FROM {}").format(history)
        )
        total, failed = totals.rows[0]
        recent = await run_query(
            target,
            pgsql.SQL(
                "SELECT version, description, success, installed_on FROM {} ORDER BY installed_rank DESC LIMIT {}"
            ).format(history, pgsql.Literal(RECENT_MIGRATIONS)),
        )
        status = "✅" if failed == 0 else f"❌ {failed} failed"
        lines += ["", f"{schema}: {total} migration(s) {status}"]
        for version, description, success, installed_on in recent.rows:
            mark = "" if success else "  ❌ FAILED"
            lines.append(f"  V{cell(version)}  {cell(description)}  ({cell(installed_on)}){mark}")
    return "\n".join(lines)
