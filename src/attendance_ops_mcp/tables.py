from datetime import date, datetime
from typing import Any

MAX_CELL_LENGTH = 80


def cell(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<{len(value)} bytes>"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    text = " ".join(str(value).split())
    return text if len(text) <= MAX_CELL_LENGTH else text[: MAX_CELL_LENGTH - 1] + "…"


def format_table(header: list[str], rows: list[list[str]]) -> list[str]:
    widths = [max(len(row[i]) for row in [header, *rows]) for i in range(len(header))]
    return ["  ".join(text.ljust(width) for text, width in zip(row, widths)).rstrip() for row in [header, *rows]]
