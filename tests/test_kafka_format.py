from kafka import TopicPartition
from kafka.consumer.fetcher import ConsumerRecord

from attendance_ops_mcp.tools.kafka import compact_partitions, decode, render_record


def record(value: bytes | None, key: bytes | None = b"user-7", headers: list | None = None) -> ConsumerRecord:
    return ConsumerRecord(
        topic="att.month-statistic", partition=2, leader_epoch=0, offset=41,
        timestamp=1762934400000, timestamp_type=0, key=key, value=value, headers=headers or [],
        checksum=None, serialized_key_size=6, serialized_value_size=0, serialized_header_size=0,
    )


def test_decode_compacts_json_and_handles_text_binary_and_null() -> None:
    assert decode(b'{\n  "tenantId": 7,\n  "month": "2026-11"\n}') == '{"tenantId":7,"month":"2026-11"}'
    assert decode(b"plain   text") == "plain text"
    assert decode(b"\xff\xfe\x00") == "<3 bytes>"
    assert decode(None) == "null"


def test_render_record_shows_position_time_key_headers_and_value() -> None:
    lines = render_record(record(b'{"a": 1}', headers=[("__TypeId__", b"x.Y")]))
    assert lines == [
        "p2@41  2025-11-12 08:00:00  key=user-7  headers=__TypeId__",
        '    {"a":1}',
    ]


def test_render_record_truncates_long_values() -> None:
    value = render_record(record(b"x" * 5000))[1]
    assert len(value.strip()) == 600
    assert value.endswith("…")


def test_compact_partitions_groups_by_topic() -> None:
    partitions = [TopicPartition("b", 1), TopicPartition("a", 3), TopicPartition("a", 0)]
    assert compact_partitions(partitions) == "a[0,3], b[1]"
    assert compact_partitions([]) == "-"
