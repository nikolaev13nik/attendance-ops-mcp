import re
from dataclasses import dataclass

from attendance_ops_mcp.logformat import context_text, parse_json_line, short_logger, single_line

LEVEL_RANK = {"TRACE": 0, "DEBUG": 1, "INFO": 2, "WARN": 3, "WARNING": 3, "ERROR": 4, "FATAL": 5}
PLAIN_LEVEL = re.compile(r"\b(FATAL|ERROR|WARNING|WARN)\b:?\s*(.*)")
NUMBERS = re.compile(r"\d+")
CAUSED_BY = "Caused by: "
MESSAGE_LIMIT = 200
FINGERPRINT_MESSAGE_LIMIT = 120
MAX_OWN_FRAMES = 3


@dataclass(frozen=True)
class Problem:
    level: str
    logger: str
    message: str
    chain: tuple[str, ...] = ()
    root_cause: str = ""
    via: str = ""
    own_frames: tuple[str, ...] = ()
    timestamp: str = ""
    context: str = ""

    @property
    def fingerprint(self) -> tuple[str, ...]:
        if self.chain:
            first_own_frame = self.own_frames[0] if self.own_frames else ""
            return ("stack", self.chain[0], self.chain[-1], first_own_frame)
        return ("message", self.level, self.logger, NUMBERS.sub("#", self.message)[:FINGERPRINT_MESSAGE_LIMIT])


@dataclass
class Group:
    service: str
    example: Problem
    count: int = 0
    first: str = ""
    last: str = ""
    before_restart: bool = False


def simple_class(header: str) -> str:
    return header.split(":", 1)[0].strip().rsplit(".", 1)[-1]


def describe_header(header: str) -> str:
    message = header.partition(":")[2].strip()
    text = f"{simple_class(header)}: {message}" if message else simple_class(header)
    return single_line(text)[:MESSAGE_LIMIT]


def short_frame(frame: str) -> str:
    name, _, location = frame.partition("(")
    short_name = ".".join(name.split(".")[-2:])
    return f"{short_name}({location}" if location else short_name


def analyze_stack(stack_trace: str, own_package: str) -> tuple[tuple[str, ...], str, str, tuple[str, ...]]:
    lines = stack_trace.splitlines()
    headers = [lines[0], *(line.removeprefix(CAUSED_BY) for line in lines if line.startswith(CAUSED_BY))]
    chain = tuple(simple_class(header) for header in headers)
    root_cause = describe_header(headers[-1])
    via = describe_header(headers[-2]) if len(headers) > 1 else ""
    frames: list[str] = []
    for line in lines:
        text = line.strip()
        if text.startswith(f"at {own_package}"):
            frame = short_frame(text.removeprefix("at "))
            if frame not in frames:
                frames.append(frame)
    return chain, root_cause, via, tuple(frames[:MAX_OWN_FRAMES])


def problem_from_line(line: str, own_package: str, min_rank: int) -> Problem | None:
    entry = parse_json_line(line)
    if entry is None:
        match = PLAIN_LEVEL.search(line)
        if match is None or LEVEL_RANK[match.group(1)] < min_rank:
            return None
        return Problem(level=match.group(1), logger="-", message=single_line(match.group(2))[:MESSAGE_LIMIT])
    level = str(entry.get("severity") or entry.get("level") or "").upper()
    if LEVEL_RANK.get(level, 0) < min_rank:
        return None
    stack_trace = str(entry.get("stack_trace", ""))
    chain, root_cause, via, own_frames = analyze_stack(stack_trace, own_package) if stack_trace else ((), "", "", ())
    return Problem(
        level=level,
        logger=short_logger(str(entry.get("logger_name", "-"))),
        message=single_line(str(entry.get("message", "")))[:MESSAGE_LIMIT],
        chain=chain,
        root_cause=root_cause,
        via=via,
        own_frames=own_frames,
        timestamp=str(entry.get("@timestamp", "")),
        context=context_text(entry),
    )


def group_problems(found: list[tuple[str, bool, Problem]]) -> list[Group]:
    groups: dict[tuple[str, ...], Group] = {}
    for service, before_restart, problem in found:
        group = groups.setdefault((service, *problem.fingerprint), Group(service, problem))
        group.count += 1
        group.before_restart = group.before_restart or before_restart
        if problem.timestamp:
            group.first = min(group.first or problem.timestamp, problem.timestamp)
            if problem.timestamp >= group.last:
                group.last = problem.timestamp
                group.example = problem
    return sorted(groups.values(), key=lambda group: group.count, reverse=True)
