import json
from typing import Any

CONTEXT_FIELDS = {"tenantId": "tenant", "userId": "user"}
TRACE_ID_LENGTH = 8


def parse_json_line(line: str) -> dict[str, Any] | None:
    stripped = line.strip()
    if stripped[:1] != "{":
        return None
    try:
        value = json.loads(stripped)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def short_logger(name: str) -> str:
    return name.rsplit(".", 1)[-1]


def single_line(text: str) -> str:
    return " ".join(text.split())


def context_text(entry: dict[str, Any]) -> str:
    context = [f"{label}={entry[field]}" for field, label in CONTEXT_FIELDS.items() if field in entry]
    if "traceId" in entry:
        context.append(f"trace={str(entry['traceId'])[:TRACE_ID_LENGTH]}")
    return " ".join(context)


def compact_entry(entry: dict[str, Any]) -> str:
    time = str(entry.get("@timestamp", ""))[11:19]
    level = str(entry.get("severity") or entry.get("level") or "")
    logger = short_logger(str(entry.get("logger_name", "")))
    message = single_line(str(entry.get("message", "")))
    context = context_text(entry)
    line = f"{time} {level:<5} {logger}  {message}"
    if context:
        line += f"  [{context}]"
    stack_trace = str(entry.get("stack_trace", ""))
    if stack_trace:
        line += f"  ↳ {single_line(stack_trace.splitlines()[0])}"
    return line


def compact_line(line: str) -> str:
    entry = parse_json_line(line)
    return line if entry is None else compact_entry(entry)
