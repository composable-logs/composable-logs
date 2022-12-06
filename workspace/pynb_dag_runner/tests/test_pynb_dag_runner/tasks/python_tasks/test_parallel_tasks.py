import time

#
import pytest

#
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.helpers import range_intersect
from pynb_dag_runner.core.dag_runner import (
    start_and_await_tasks,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)

from .py_test_helpers import get_time_range


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:
        tasks = [
            task_from_python_function(
                lambda _: time.sleep(1.0),
                attributes={"task.function_id": f"id#{function_id}"},
                timeout_s=10.0,
            )
            for function_id in range(2)
        ]

        _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")
    return rec.spans


def test__python_task__run_in_parallel(spans: Spans):
    assert len(spans.filter(["name"], "execute-task")) == 2

    t0_us_range = get_time_range(spans, "id#0", inner=False)
    t1_us_range = get_time_range(spans, "id#1", inner=False)

    # Check: since there are no order constraints, the time ranges should
    # overlap provided tests are run on 2+ CPUs
    assert range_intersect(t0_us_range, t1_us_range)

    # assert_compatibility(spans)
