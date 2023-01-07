from functools import lru_cache

#
import ray
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
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans

from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    Spans,
    SpanDict,
    SpanRecorder,
)
from pynb_dag_runner.wrappers import task, run_dag

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

    @task(task_id="task-f")
    def f():
        assert ray.get(call_counter.get_count.remote()) == 1

        raise Exception("BOOM-2000")

    with SpanRecorder() as rec:
        try:
            run_dag(dag=f())
            raise Exception("run_dag should raise an exception")

        except Exception as e:
            assert "BOOM-2000" in str(e)

    return rec.spans


def test_spans_a_failed_task_is_not_retried(spans_a_failed_task_is_not_retried: Spans):
    # TODO: currently the exceptions is logged three times in the
    # OpenTelemetry logs and not just once.
    #
    # A first question would be: are the exceptions logged to different of the same
    # span?
    #
    # for x in rec.spans.exception_events():
    #     import json
    #     print(120 * "=")
    #     print(json.dumps(x, indent=2))
    #
    # In particular, this is different from exception count below
    all_exceptions = spans_a_failed_task_is_not_retried.exception_events()
    assert len(all_exceptions) == 3
    for e in all_exceptions:
        assert "BOOM-2000" in str(e)

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
