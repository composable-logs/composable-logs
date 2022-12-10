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
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_task_artifact_iterators,
)

# -
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


def test__python_task__parallel_tasks__run_in_parallel(spans: Spans):
    assert len(spans.filter(["name"], "execute-task")) == 2

    t0_us_range = get_time_range(spans, "id#0", inner=True)
    t1_us_range = get_time_range(spans, "id#1", inner=True)

    # Check: since there are no order constraints, the time ranges should
    # overlap provided tests are run on 2+ CPUs
    assert range_intersect(t0_us_range, t1_us_range)


def test__python_task__parallel_tasks__parse_spans(spans: Spans):
    _, task_run_it = get_pipeline_task_artifact_iterators(spans)

    # check attributes
    ids = []
    for task_run_summary, artefacts in task_run_it:  # type: ignore
        assert len(artefacts) == 0
        ids.append(task_run_summary.attributes["task.function_id"])

    assert set(ids) == {"id#0", "id#1"}

    # check that
