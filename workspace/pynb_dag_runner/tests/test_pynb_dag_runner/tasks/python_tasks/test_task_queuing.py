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


def test__python_task__parallel_tasks_are_queued_based_on_available_ray_worker_cpus():
    def get_test_spans():
        with SpanRecorder() as rec:
            tasks = [
                task_from_python_function(
                    lambda _: time.sleep(0.5),
                    attributes={"task.function_id": f"id#{function_id}"},
                    timeout_s=10.0,
                )
                for function_id in range(4)
            ]

            start_ts = time.time_ns()
            _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")
            end_ts = time.time_ns()

            # Check 1: with only 2 CPU:s (reserved for unit tests, see ray.init call)
            # running the above tasks with no constraints should take > 1 secs.
            duration_ms = (end_ts - start_ts) // 1000000
            assert duration_ms >= 1000, duration_ms
        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "execute-task")) == 4

        task_runtime_ranges = [
            get_time_range(spans, span_id, inner=True)
            for span_id in [f"id#{function_id}" for function_id in range(4)]
        ]

        # Check 2: since only 2 CPU:s are reserved (for unit tests, see above)
        # the intersection of three runtime ranges should always be empty.
        for r1, r2, r3 in itertools.combinations(task_runtime_ranges, 3):
            assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))

        # assert_compatibility(spans)

    validate_spans(get_test_spans())
