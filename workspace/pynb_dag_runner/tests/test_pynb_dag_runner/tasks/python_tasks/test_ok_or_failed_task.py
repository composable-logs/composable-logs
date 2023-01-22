from functools import lru_cache

# -
import ray
import pytest

# -
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.helpers import one, Failure
from pynb_dag_runner.core.dag_runner import (
    TaskOutcome,
    start_and_await_tasks,
    RemoteTaskP,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans

from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    Spans,
    SpanDict,
    SpanRecorder,
)
from pynb_dag_runner.wrappers import task, run_dag, ExceptionGroup

# Error message for failing tasks
ERROR_MSG = "!!!Exception-12342!!!"


@lru_cache
def get_spans(task_should_fail: bool) -> Spans:
    """
    Get test spans for Python task that fails according to task_should_fail.

    Note:
    pytest can parameterise fixtures, but it seems complicated to also keep
    track of both input parameter and evaluated fixture. Therefore, we get
    spans manually in each test.

    https://docs.pytest.org/en/stable/example/parametrize.html#indirect-parametrization

    """
    assert False
    with SpanRecorder() as rec:

        def f(_):
            if task_should_fail:
                raise Exception(ERROR_MSG)
            else:
                return 123

        task: RemoteTaskP = task_from_python_function(
            f, attributes={"pipeline.foo": "bar", "task.foo": "my_test_func"}
        )
        [outcome] = start_and_await_tasks(
            [task], [task], timeout_s=10, arg="dummy value"
        )

        # check Task outcome
        assert isinstance(outcome, TaskOutcome)
        if task_should_fail:
            assert ERROR_MSG in str(outcome.error)
        else:
            assert outcome.return_value == 123

    return rec.spans


@pytest.mark.skipif(True, reason="remove after move to new Ray interface")
@pytest.mark.parametrize("task_should_fail", [True, False])
def test__python_task__ok_or_fail__parsed_spans(task_should_fail: bool):
    spans = get_spans(task_should_fail)  # manually get spans for parameter

    pipeline_summary = parse_spans(spans)

    assert pipeline_summary.task_dependencies == set()
    assert pipeline_summary.attributes == {"pipeline.foo": "bar"}

    for task_summary in [one(pipeline_summary.task_runs)]:  # type: ignore
        assert len(task_summary.logged_values) == 0
        assert len(task_summary.logged_artifacts) == 0

        assert task_summary.is_success() == (not task_should_fail)
        assert task_summary.attributes == {
            "pipeline.foo": "bar",
            "task.foo": "my_test_func",
            "task.num_cpus": 1,
            "task.task_type": "Python",
        }

        if task_should_fail:
            # now two exceptions: same exception is raised in function and in runner
            assert len(task_summary.exceptions) == 2

            for e in task_summary.exceptions:
                assert e["attributes"]["exception.message"] == ERROR_MSG


@pytest.mark.skipif(True, reason="move away from this file")
def test__python_task__ok_or_fail__validate_spans():
    spans = get_spans(task_should_fail=False)

    assert len(spans.filter(["name"], "task-dependency")) == 0

    top_task_span: SpanDict = one(spans.filter(["name"], "execute-task"))
    assert read_key(top_task_span, ["attributes", "task.foo"]) == "my_test_func"
    assert read_key(top_task_span, ["attributes", "task.task_type"]) == "Python"

    assert len(spans.bound_inclusive(top_task_span).exception_events()) == 0

    # --- check timeout-guard span ---
    timeout_span: SpanDict = one(spans.filter(["name"], "timeout-guard"))
    assert timeout_span["status"] == {"status_code": "OK"}  # no timeouts

    # --- check call-python-function span ---
    call_function_span: SpanDict = one(spans.filter(["name"], "call-python-function"))
    assert call_function_span["status"] == {"status_code": "OK"}

    # check nesting of above spans
    assert spans.contains_path(
        top_task_span,
        timeout_span,
        call_function_span,
    )


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
    pipeline_summary = parse_spans(spans_a_failed_task_is_not_retried)

    assert pipeline_summary.attributes == {}

    assert len(pipeline_summary.task_runs) == 1

    for task_summary in pipeline_summary.task_runs:  # type: ignore
        assert not task_summary.is_success()
        assert len(task_summary.exceptions) == 1
        assert "BOOM-2000" in str(task_summary.exceptions)

    # check logged task dependencies
    assert len(pipeline_summary.task_dependencies) == 0


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
    pipeline_summary = parse_spans(spans_middle_task_fails)

    assert pipeline_summary.attributes == {}

    assert len(pipeline_summary.task_runs) == 2

    for task_summary in pipeline_summary.task_runs:  # type: ignore
        if task_summary.task_id == "mid-task-f":
            assert task_summary.is_success()
        elif task_summary.task_id == "mid-task-g":
            assert not task_summary.is_success()
            assert len(task_summary.exceptions) == 1
            assert "middle task failed" in str(task_summary.exceptions)
        else:
            raise Exception("Unknown task-id")

    # check logged task dependencies
    assert len(pipeline_summary.task_dependencies) == 1
