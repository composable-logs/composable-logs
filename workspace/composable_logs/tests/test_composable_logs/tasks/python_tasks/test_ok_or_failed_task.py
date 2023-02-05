# -
import ray
import pytest

# -
from composable_logs.opentelemetry_helpers import Spans
from composable_logs.helpers import one, Failure
from composable_logs.opentelemetry_task_span_parser import parse_spans

from composable_logs.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)
from composable_logs.wrappers import task, run_dag, ExceptionGroup

# Error message for failing tasks
ERROR_MSG = "!!!Exception-12342!!!"


# --- assert that tasks are not retried by Ray ---


@pytest.fixture(scope="module")
def spans_a_failed_task_is_not_retried() -> Spans:
    @ray.remote
    class CallCounter:
        def __init__(self):
            self.count = 0

        def get_count(self):
            self.count += 1
            return self.count

    call_counter = CallCounter.remote()  # type: ignore

    test_exception = Exception("BOOM-2000")

    @task(task_id="task-f")
    def f():
        assert ray.get(call_counter.get_count.remote()) == 1
        raise test_exception

    with SpanRecorder() as rec:
        assert run_dag(dag=f()) == Failure(ExceptionGroup([test_exception]))

    return rec.spans


def test_spans_a_failed_task_is_not_retried(spans_a_failed_task_is_not_retried: Spans):
    assert "BOOM-2000" in str(
        one(spans_a_failed_task_is_not_retried.exception_events())
    )

    # Check parsed spans
    workflow_summary = parse_spans(spans_a_failed_task_is_not_retried)

    assert workflow_summary.attributes == {}

    assert len(workflow_summary.task_runs) == 1

    for task_summary in workflow_summary.task_runs:  # type: ignore
        assert not task_summary.is_success()
        assert len(task_summary.exceptions) == 1
        assert "BOOM-2000" in str(task_summary.exceptions)

    # check logged task dependencies
    assert len(workflow_summary.task_dependencies) == 0


# --- error handling in case middle task fails in workflow ---


@pytest.fixture(scope="module")
def spans_middle_task_fails() -> Spans:
    test_exception = Exception("middle task failed")

    # Check error handling for below DAG when task-g throws an exception:
    #
    #   task-f  --->  task_g  --->  task-h
    #

    @task(task_id="mid-task-f")
    def f():
        pass

    @task(task_id="mid-task-g")
    def g(_):
        raise test_exception

    @task(task_id="mid-task-h")
    def h(_):
        raise Exception("this should never be executed")

    with SpanRecorder() as rec:
        dag_result = run_dag(dag=h(g(f())))
        assert dag_result == Failure(ExceptionGroup([test_exception]))
        assert "this should never be executed" not in str(dag_result)

    return rec.spans


def test_spans_middle_task_fails(spans_middle_task_fails: Spans):

    assert "middle task failed" in str(spans_middle_task_fails.spans)
    assert "this should never be executed" not in str(spans_middle_task_fails.spans)

    # Check parsed spans
    workflow_summary = parse_spans(spans_middle_task_fails)

    assert workflow_summary.attributes == {}

    assert len(workflow_summary.task_runs) == 2

    for task_summary in workflow_summary.task_runs:  # type: ignore
        if task_summary.task_id == "mid-task-f":
            assert task_summary.is_success()
        elif task_summary.task_id == "mid-task-g":
            assert not task_summary.is_success()
            assert len(task_summary.exceptions) == 1
            assert "middle task failed" in str(task_summary.exceptions)
        else:
            raise Exception("Unknown task-id")

    # check logged task dependencies
    assert len(workflow_summary.task_dependencies) == 1
