import json
import re
import time
from datetime import datetime, timezone
from typing import Annotated, Any

from kafka import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from attendance_ops_mcp.config import ChosenEnv, EnvParam, target_for
from attendance_ops_mcp.kafka_client import new_admin, new_consumer, with_kafka
from attendance_ops_mcp.server import mcp
from attendance_ops_mcp.tables import format_table

READ_ONLY = ToolAnnotations(readOnlyHint=True)
KAFKA_NAME = re.compile(r"[A-Za-z0-9._-]{1,249}")
MAX_MESSAGES = 100
FILTER_SCAN_PER_PARTITION = 1000
READ_TIMEOUT_SECONDS = 20
MAX_VALUE_LENGTH = 600


def check_name(value: str, what: str) -> str:
    if KAFKA_NAME.fullmatch(value) is None:
        raise ToolError(f"'{value}' is not a valid Kafka {what} name.")
    return value


def partitions_of(consumer: Any, topic: str) -> list[TopicPartition]:
    if topic not in consumer.topics():
        raise ToolError(f"Topic '{topic}' does not exist. Call list_topics.")
    numbers = consumer.partitions_for_topic(topic) or set()
    return [TopicPartition(topic, number) for number in sorted(numbers)]


def decode(raw: bytes | None) -> str:
    if raw is None:
        return "null"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return f"<{len(raw)} bytes>"
    try:
        return json.dumps(json.loads(text), separators=(",", ":"), ensure_ascii=False)
    except json.JSONDecodeError:
        return " ".join(text.split())


def render_record(record: ConsumerRecord) -> list[str]:
    when = datetime.fromtimestamp(record.timestamp / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    header_names = [name for name, _ in record.headers or []]
    headers = f"  headers={','.join(header_names)}" if header_names else ""
    value = decode(record.value)
    if len(value) > MAX_VALUE_LENGTH:
        value = value[: MAX_VALUE_LENGTH - 1] + "…"
    return [f"p{record.partition}@{record.offset}  {when}  key={decode(record.key)}{headers}", f"    {value}"]


@mcp.tool(
    description=(
        "List Kafka topics (internal ones hidden) with partition count, messages still retained and "
        "total messages ever written."
    ),
    annotations=READ_ONLY,
)
async def list_topics(choice: ChosenEnv, env: EnvParam = None) -> str:
    target = target_for(choice.env)

    def collect(bootstrap: str) -> list[list[str]]:
        consumer = new_consumer(bootstrap)
        try:
            rows = []
            for topic in sorted(consumer.topics()):
                if topic.startswith("__"):
                    continue
                partitions = partitions_of(consumer, topic)
                begin, end = consumer.beginning_offsets(partitions), consumer.end_offsets(partitions)
                retained = sum(end[p] - begin[p] for p in partitions)
                rows.append([topic, str(len(partitions)), str(retained), str(sum(end.values()))])
            return rows
        finally:
            consumer.close()

    rows = await with_kafka(target, collect)
    return "\n".join([f"{target.label} {len(rows)} topic(s)", *format_table(["TOPIC", "PARTITIONS", "RETAINED", "TOTAL_WRITTEN"], rows)])


@mcp.tool(
    description=(
        f"Read the last messages of a Kafka topic (at most {MAX_MESSAGES}), newest last, with "
        "partition@offset, time, key, header names and the value (JSON compacted). Never joins a "
        "consumer group or commits offsets, so the services are not affected. Filter with key "
        f"(exact) or contains (case-insensitive text); filters scan up to {FILTER_SCAN_PER_PARTITION} "
        "recent messages per partition."
    ),
    annotations=READ_ONLY,
)
async def read_messages(
    choice: ChosenEnv,
    topic: Annotated[str, Field(description="Topic name, see list_topics")],
    env: EnvParam = None,
    last_n: Annotated[int, Field(description=f"How many messages, 1-{MAX_MESSAGES}")] = 20,
    key: Annotated[str | None, Field(description="Only messages with exactly this key")] = None,
    contains: Annotated[str | None, Field(description="Only messages whose value contains this text")] = None,
    partition: Annotated[int | None, Field(description="Only this partition")] = None,
) -> str:
    target = target_for(choice.env)
    check_name(topic, "topic")
    last_n = min(max(last_n, 1), MAX_MESSAGES)
    filtered = key is not None or contains is not None

    def matches(record: ConsumerRecord) -> bool:
        if key is not None and decode(record.key) != key:
            return False
        return contains is None or contains.lower() in decode(record.value).lower()

    def collect(bootstrap: str) -> tuple[list[ConsumerRecord], bool]:
        consumer = new_consumer(bootstrap)
        try:
            partitions = [p for p in partitions_of(consumer, topic) if partition is None or p.partition == partition]
            if len(partitions) == 0:
                raise ToolError(f"Topic '{topic}' has no partition {partition}.")
            begin, end = consumer.beginning_offsets(partitions), consumer.end_offsets(partitions)
            window = FILTER_SCAN_PER_PARTITION if filtered else last_n
            consumer.assign(partitions)
            for p in partitions:
                consumer.seek(p, max(begin[p], end[p] - window))
            pending = {p for p in partitions if max(begin[p], end[p] - window) < end[p]}
            records: list[ConsumerRecord] = []
            deadline = time.monotonic() + READ_TIMEOUT_SECONDS
            while pending and time.monotonic() < deadline:
                for tp, batch in consumer.poll(timeout_ms=1000, max_records=500).items():
                    records += [record for record in batch if record.offset < end[tp] and matches(record)]
                pending = {p for p in pending if consumer.position(p) < end[p]}
            return records, len(pending) > 0
        finally:
            consumer.close()

    records, incomplete = await with_kafka(target, collect)
    selected = sorted(records, key=lambda r: (r.timestamp, r.partition, r.offset))[-last_n:]
    conditions = [f"key={key}"] * (key is not None) + [f"contains '{contains}'"] * (contains is not None)
    where = f" matching {' and '.join(conditions)}" if conditions else ""
    lines = [f"{target.label} {topic}: {len(selected)} message(s){where}"]
    if incomplete:
        lines.append(f"⚠️  stopped after {READ_TIMEOUT_SECONDS}s; some partitions were not fully read")
    for record in selected:
        lines += render_record(record)
    return "\n".join(lines)


def assignment_of(member: dict[str, Any]) -> list[TopicPartition]:
    assignment = member.get("member_assignment")
    if isinstance(assignment, dict):
        items = assignment.get("assigned_partitions") or []
        return [TopicPartition(item["topic"], number) for item in items for number in item["partitions"]]
    if hasattr(assignment, "partitions"):
        return list(assignment.partitions())
    return []


def compact_partitions(partitions: list[TopicPartition]) -> str:
    by_topic: dict[str, list[int]] = {}
    for tp in partitions:
        by_topic.setdefault(tp.topic, []).append(tp.partition)
    return ", ".join(f"{topic}[{','.join(str(n) for n in sorted(numbers))}]" for topic, numbers in sorted(by_topic.items())) or "-"


@mcp.tool(
    description=(
        "Consumer groups with state, members (client id, host, assigned partitions) and lag per "
        "topic and partition (end offset minus committed offset). Also flags partitions that have "
        "committed offsets but no member assigned. Use it for 'are events stuck?' and rebalance problems."
    ),
    annotations=READ_ONLY,
)
async def consumer_lag(
    choice: ChosenEnv,
    env: EnvParam = None,
    group: Annotated[str | None, Field(description="Only this consumer group. Default: all groups")] = None,
) -> str:
    target = target_for(choice.env)
    if group is not None:
        check_name(group, "group")

    def collect(bootstrap: str) -> list[str]:
        admin, consumer = new_admin(bootstrap), new_consumer(bootstrap)
        try:
            group_ids = [group] if group is not None else sorted(g["group_id"] for g in admin.list_groups())
            if len(group_ids) == 0:
                return ["no consumer groups"]
            descriptions = admin.describe_groups(group_ids)
            offsets = admin.list_group_offsets(group_ids)
            lines: list[str] = []
            for group_id in group_ids:
                description = descriptions.get(group_id, {})
                committed: dict[TopicPartition, Any] = offsets.get(group_id, {})
                members = description.get("members", [])
                assigned = [tp for member in members for tp in assignment_of(member)]
                lines += ["", f"{group_id}  state={description.get('group_state', '?')}  members={len(members)}"]
                for member in members:
                    lines.append(
                        f"  member {member.get('client_id', '?')} @ {member.get('client_host', '?')}: "
                        f"{compact_partitions(assignment_of(member))}"
                    )
                topics = sorted({tp.topic for tp in [*committed, *assigned]})
                for topic in topics:
                    partitions = partitions_of(consumer, topic)
                    end = consumer.end_offsets(partitions)
                    rows, total_lag, uncommitted = [], 0, []
                    for tp in partitions:
                        meta = committed.get(tp)
                        if meta is None or meta.offset < 0:
                            uncommitted.append(tp)
                            continue
                        lag = end[tp] - meta.offset
                        total_lag += lag
                        if lag > 0:
                            rows.append(f"    p{tp.partition}: committed {meta.offset}, end {end[tp]}, lag {lag}")
                    lines.append(f"  {topic}: total lag {total_lag}")
                    lines += rows
                    unread = [tp for tp in uncommitted if end[tp] > 0]
                    if unread:
                        lines.append(f"    ⚠️  never committed but has messages: {compact_partitions(unread)}")
                    if len(uncommitted) > len(unread):
                        lines.append(f"    no messages and no commits yet: {len(uncommitted) - len(unread)} partition(s)")
                orphaned = [tp for tp in committed if tp not in assigned] if members else []
                if orphaned:
                    lines.append(f"  ⚠️  committed but not assigned to any member: {compact_partitions(orphaned)}")
            return lines
        finally:
            consumer.close()
            admin.close()

    lines = await with_kafka(target, collect)
    return "\n".join([f"{target.label} consumer groups", *lines])
