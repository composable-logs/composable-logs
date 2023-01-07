import time

#
import pytest

#
from pynb_dag_runner.helpers import range_intersect
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans
from pynb_dag_runner.wrappers import task, run_dag


@pytest.fixture(scope="module")
def spans() -> Spans:
    @task(task_id=f"f-#1", num_cpus=1)
    def f1():
        time.sleep(1.0)

    @task(task_id=f"f-#2", num_cpus=1)
    def f2():
        time.sleep(1.0)

    with SpanRecorder() as rec:
        run_dag(dag=[f1(), f2()])

    return rec.spans


def test__python_task__parallel_tasks__parse_spans(spans: Spans):
    pipeline_summary = parse_spans(spans)

    # check attributes
    ids = []

    ranges = []
    for task_summary in pipeline_summary.task_runs:  # type: ignore
        assert task_summary.is_success()
        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        assert task_summary.task_id == task_summary.attributes["task.task_id"]
        ids.append(task_summary.attributes["task.task_id"])
        ranges.append(task_summary.timing.get_task_timestamp_range_us_epoch())

    # Check: since there are no order constraints, the time ranges should
    # overlap provided tests are run on 2+ CPU cores
    assert set(ids) == {"f-#1", "f-#2"}
    assert range_intersect(*ranges)
