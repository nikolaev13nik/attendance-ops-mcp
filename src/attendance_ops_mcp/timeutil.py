from datetime import datetime, timezone


def parse_time(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    return datetime.fromisoformat(value)


def minutes_since(moment: datetime) -> float:
    return (datetime.now(timezone.utc) - moment).total_seconds() / 60


def age(value: str | None) -> str:
    moment = parse_time(value)
    if moment is None:
        return "?"
    total_minutes = int(minutes_since(moment))
    days, rest = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(rest, 60)
    if days > 0:
        return f"{days}d{hours}h"
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"
