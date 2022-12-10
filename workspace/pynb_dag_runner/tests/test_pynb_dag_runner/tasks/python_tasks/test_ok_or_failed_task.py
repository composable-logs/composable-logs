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
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_task_artifact_iterators,
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


@pytest.mark.parametrize("task_should_fail", [True, False])
def test__python_task__ok_or_fail__parsed_spans(task_should_fail: bool):
    spans = get_spans(task_should_fail)  # manually get spans for parameter

    pipeline_summary, task_it = get_pipeline_task_artifact_iterators(spans)

    assert pipeline_summary.task_dependencies == []
    expected_pipeline_attributes = {"pipeline.foo": "bar"}
    assert pipeline_summary.attributes == expected_pipeline_attributes

    for task_run_summary, artefact_it in [one(task_it)]:  # type: ignore

        assert len(task_run_summary.logged_values) == 0
        assert len(artefact_it) == 0

        if task_should_fail:
            assert task_run_summary.status["status_code"] == "ERROR"

            assert task_run_summary.attributes == {
                "pipeline.foo": "bar",
                "task.foo": "my_test_func",
                "task.max_nr_retries": 1,
                "task.num_cpus": 1,
                "task.task_type": "Python",
            }

            # now two exceptions: same exception is raised in function and in runner
            assert len(task_run_summary.status["exceptions"]) == 2

            for e in task_run_summary.status["exceptions"]:
                assert e["attributes"]["exception.message"] == ERROR_MSG

        else:
            assert task_run_summary.status == {"status_code": "OK"}


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
