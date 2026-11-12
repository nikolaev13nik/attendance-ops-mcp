import json

from attendance_ops_mcp.logformat import compact_line
from attendance_ops_mcp.problems import LEVEL_RANK, analyze_stack, group_problems, problem_from_line

OWN = "co.il."
ERROR = LEVEL_RANK["ERROR"]
WARN = LEVEL_RANK["WARN"]

STACK = "\n".join([
    "org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'entityManagerFactory'",
    "\tat org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200)",
    "\tat co.il.attendanceaccounting.AttendanceAccountingApplication.main(AttendanceAccountingApplication.java:40)",
    "Caused by: org.postgresql.util.PSQLException: Connection to postgres:5432 refused.",
    "\tat org.postgresql.Driver.connect(Driver.java:300)",
    "Caused by: java.net.ConnectException: Connection refused",
    "\tat java.base/sun.nio.ch.Net.connect0(Native Method)",
    "\t... 42 common frames omitted",
])


def json_line(**fields: object) -> str:
    return json.dumps({"@timestamp": "2026-11-08T08:16:24.979Z", "logger_name": "a.b.SpringApplication", **fields})


def test_analyze_stack_finds_chain_root_cause_and_own_frames() -> None:
    chain, root_cause, via, own_frames = analyze_stack(STACK, OWN)
    assert chain == ("BeanCreationException", "PSQLException", "ConnectException")
    assert root_cause == "ConnectException: Connection refused"
    assert via == "PSQLException: Connection to postgres:5432 refused."
    assert own_frames == ("AttendanceAccountingApplication.main(AttendanceAccountingApplication.java:40)",)


def test_stack_without_caused_by_is_its_own_root_cause() -> None:
    chain, root_cause, via, _ = analyze_stack("java.lang.IllegalStateException: boom\n\tat x.Y.z(Y.java:1)", OWN)
    assert chain == ("IllegalStateException",)
    assert root_cause == "IllegalStateException: boom"
    assert via == ""


def test_json_error_line_becomes_problem() -> None:
    line = json_line(severity="ERROR", message="Application run failed", stack_trace=STACK, tenantId="7", traceId="6af577935a84")
    problem = problem_from_line(line, OWN, ERROR)
    assert problem is not None
    assert problem.logger == "SpringApplication"
    assert problem.root_cause == "ConnectException: Connection refused"
    assert problem.context == "tenant=7 trace=6af57793"


def test_lines_below_min_level_are_ignored() -> None:
    assert problem_from_line(json_line(severity="INFO", message="started"), OWN, ERROR) is None
    assert problem_from_line(json_line(severity="WARN", message="slow"), OWN, ERROR) is None
    assert problem_from_line(json_line(severity="WARN", message="slow"), OWN, WARN) is not None


def test_plain_text_postgres_error_is_detected() -> None:
    line = '2026-11-08 13:27:06.535 UTC [40342] ERROR:  relation "accounting.user" does not exist'
    problem = problem_from_line(line, OWN, ERROR)
    assert problem is not None
    assert problem.message == 'relation "accounting.user" does not exist'


def test_same_exception_groups_and_numbers_do_not_split_messages() -> None:
    crash = problem_from_line(json_line(severity="ERROR", message="failed", stack_trace=STACK), OWN, ERROR)
    slow_a = problem_from_line(json_line(severity="WARN", message="request took 812 ms"), OWN, WARN)
    slow_b = problem_from_line(json_line(severity="WARN", message="request took 1530 ms"), OWN, WARN)
    groups = group_problems([("accounting", False, crash), ("accounting", True, crash), ("gui", False, slow_a), ("gui", False, slow_b)])
    assert [(g.service, g.count) for g in groups] == [("accounting", 2), ("gui", 2)]
    assert groups[0].before_restart is True


def test_compact_line_shortens_json_and_keeps_plain_text() -> None:
    line = json_line(severity="INFO", message="GET /x status=200", **{"@version": "1", "level_value": 20000, "tenantId": "0"})
    assert compact_line(line) == "08:16:24 INFO  SpringApplication  GET /x status=200  [tenant=0]"
    assert compact_line("plain text line") == "plain text line"
