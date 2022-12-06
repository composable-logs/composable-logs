import time, random, itertools
from typing import List, Set, Tuple

#
import pytest
import opentelemetry as otel

#
from pynb_dag_runner.opentelemetry_helpers import SpanId, Spans
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies
from pynb_dag_runner.helpers import (
    one,
    pairs,
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
)
from pynb_dag_runner.core.dag_runner import (
    TaskOutcome,
    fan_in,
    run_in_sequence,
    start_and_await_tasks,
    RemoteTaskP,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    get_duration_range_us,
    read_key,
    get_span_id,
    get_span_exceptions,
    Spans,
    SpanDict,
    SpanRecorder,
)

from .py_test_helpers import get_time_range


def test__python_function_task__otel_logs_for_stuck_task():
    def get_test_spans():
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

    def validate_spans(spans: Spans):
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
        assert spans.contains_path(
            top_task_span,
            timeout_span,
        )

        # assert_compatibility(spans)

    validate_spans(get_test_spans())
