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


def test__python_task__parallel_tasks__parse_spans(spans: Spans):
    _, task_run_it = get_pipeline_task_artifact_iterators(spans)

    # check attributes
    ids = []

    ranges = []
    for task_run_summary, artefacts in task_run_it:  # type: ignore
        assert task_run_summary.is_success
        assert len(artefacts) == 0

        ids.append(task_run_summary.attributes["task.function_id"])
        ranges.append(task_run_summary.time_range_epoch_us())

    # both tasks were run and found in logs
    assert set(ids) == {"id#0", "id#1"}

    # Check: since there are no order constraints, the time ranges should
    # overlap provided tests are run on 2+ CPU cores
    assert range_intersect(*ranges)
