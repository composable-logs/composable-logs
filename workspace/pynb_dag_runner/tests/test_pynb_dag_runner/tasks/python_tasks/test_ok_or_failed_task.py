from functools import lru_cache

#
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
from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    get_span_exceptions,
    Spans,
    SpanDict,
    SpanRecorder,
)

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
            f, attributes={"task.foo": "my_test_func"}
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


@pytest.mark.parametrize("task_should_fail", [True, False])
def test__python_task__ok_or_fail__validate_spans(task_should_fail: bool):
    spans = get_spans(task_should_fail)  # manually get spans for parameter

    assert len(spans.filter(["name"], "task-dependency")) == 0

    top_task_span: SpanDict = one(spans.filter(["name"], "execute-task"))
    assert read_key(top_task_span, ["attributes", "task.foo"]) == "my_test_func"
    assert read_key(top_task_span, ["attributes", "task.task_type"]) == "Python"

    error_spans: Spans = Spans(
        [span for span in spans if len(get_span_exceptions(span)) > 0]
    )
    if task_should_fail:
        assert len(error_spans) > 0

        assert top_task_span["status"] == {
            "status_code": "ERROR",
            "description": "Remote function call failed",
        }
    else:
        assert len(error_spans) == 0
        assert top_task_span["status"] == {"status_code": "OK"}

    # --- check retry spans ---
    retry_wrapper_span = one(spans.filter(["name"], "retry-wrapper"))
    assert spans.contains_path(top_task_span, retry_wrapper_span)
    assert read_key(retry_wrapper_span, ["attributes", "task.max_nr_retries"]) == 1

    retry_span = one(spans.filter(["name"], "retry-call"))
    assert spans.contains_path(top_task_span, retry_wrapper_span, retry_span)
    assert read_key(retry_span, ["attributes", "run.retry_nr"]) == 0

    # --- check timeout-guard span ---
    timeout_span: SpanDict = one(spans.filter(["name"], "timeout-guard"))
    assert timeout_span["status"] == {"status_code": "OK"}  # no timeouts

    # --- check call-python-function span ---
    call_function_span: SpanDict = one(spans.filter(["name"], "call-python-function"))

    if task_should_fail:
        assert call_function_span["status"] == {
            "status_code": "ERROR",
            "description": "Failure",
        }

        # call span should record exception from function
        call_function_span_exception = one(get_span_exceptions(call_function_span))[
            "attributes"
        ]
        assert call_function_span_exception["exception.type"] == "Exception"
        assert call_function_span_exception["exception.message"] == ERROR_MSG
    else:
        assert call_function_span["status"] == {"status_code": "OK"}

    # check nesting of above spans
    assert spans.contains_path(
        top_task_span,
        retry_wrapper_span,
        retry_span,
        timeout_span,
        call_function_span,
    )
