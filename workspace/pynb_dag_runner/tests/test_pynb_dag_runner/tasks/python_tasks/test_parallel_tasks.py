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
def spans_ok() -> Spans:
    @task(task_id=f"f-#1", num_cpus=1)
    def f1():
        time.sleep(1.0)

    @task(task_id=f"f-#2", num_cpus=1)
    def f2():
        time.sleep(1.0)

    with SpanRecorder() as rec:
        run_dag(dag=[f1(), f2()])

    return rec.spans


def test__python_task__parallel_tasks__success(spans_ok: Spans):
    pipeline_summary = parse_spans(spans_ok)

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


# --- tests error handling if one of parallel tasks fail ---


@pytest.fixture(scope="module")
def spans_fail() -> Spans:
    @task(task_id="par-task-f", num_cpus=0)
    def f():
        time.sleep(0.5)

    @task(task_id="par-task-g", num_cpus=0)
    def g():
        raise Exception("task-g failure")

    @task(task_id="par-task-h", num_cpus=0)
    def h():
        time.sleep(0.5)

    with SpanRecorder() as rec:
        try:
            run_dag(dag=[f(), g(), h()])
        except:
            # TODO: without this sleep, f and h will not show up in the logs (for
            # this test). In fact, the logs for these may show up in other unit tests.
            time.sleep(4.0)

    return rec.spans


def test__python_task__parallel_tasks__fail(spans_fail: Spans):
    pipeline_summary = parse_spans(spans_fail)

    assert pipeline_summary.attributes == {}

    assert len(pipeline_summary.task_runs) == 3

    for task_summary in pipeline_summary.task_runs:  # type: ignore
        if task_summary.task_id in ["par-task-f", "par-task-h"]:
            assert task_summary.is_success()
        elif task_summary.task_id == "par-task-g":
            assert not task_summary.is_success()
            assert len(task_summary.exceptions) == 1
            assert "task-g failure" in str(task_summary.exceptions)
        else:
            raise Exception(f"Unknown task-id={task_summary.task_id}")

    assert len(pipeline_summary.task_dependencies) == 0
