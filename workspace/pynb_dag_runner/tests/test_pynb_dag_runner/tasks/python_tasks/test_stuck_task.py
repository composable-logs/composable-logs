import time
from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.helpers import one
from pynb_dag_runner.core.dag_runner import (
    TaskOutcome,
    start_and_await_tasks,
    RemoteTaskP,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    Spans,
    SpanDict,
    SpanRecorder,
)


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    with SpanRecorder() as rec:

        def f(_):
            time.sleep(1e6)

        task: RemoteTaskP = task_from_python_function(
            f, attributes={"task.id": "stuck_function"}, timeout_s=1.0
        )
        [outcome] = start_and_await_tasks(
            [task], [task], timeout_s=10, arg="dummy value"
        )

        # check Task outcome
        assert isinstance(outcome, TaskOutcome)
        assert "timeout" in str(outcome.error)
    return rec.spans


def test__python_task__otel_logs_for_stuck_task(spans: Spans):
    assert len(spans.filter(["name"], "task-dependency")) == 0

    top_task_span: SpanDict = one(spans.filter(["name"], "execute-task"))
    assert read_key(top_task_span, ["attributes", "task.id"]) == "stuck_function"
    assert read_key(top_task_span, ["attributes", "task.task_type"]) == "Python"

    # --- check timeout-guard span ---
    timeout_span: SpanDict = one(spans.filter(["name"], "timeout-guard"))
    assert read_key(timeout_span, ["attributes", "task.timeout_s"]) == 1.0

    assert timeout_span["status"] == {
        "description": "Timeout",
        "status_code": "ERROR",
    }

    # --- check call-python-function span, this should exist but is not logged ---
    assert len(spans.filter(["name"], "call-python-function")) == 0

    # check nesting of above spans
    assert spans.contains_path(top_task_span, timeout_span)

    # assert_compatibility(spans)
