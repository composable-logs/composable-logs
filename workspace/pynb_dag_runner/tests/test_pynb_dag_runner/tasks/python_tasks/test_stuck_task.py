import time

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
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:

        def f(_):
            time.sleep(1e6)

        task: RemoteTaskP = task_from_python_function(
            f, attributes={"task.id": "stuck-function"}, timeout_s=1.0
        )
        [outcome] = start_and_await_tasks(
            [task], [task], timeout_s=10, arg="dummy value"
        )

        assert isinstance(outcome, TaskOutcome)
        assert "timeout" in str(outcome.error)

    return rec.spans


def test__python_task__stuck_tasks__parse_spans(spans: Spans):
    pipeline_summary = parse_spans(spans)

    for task_summary in [one(pipeline_summary.task_runs)]:  # type: ignore
        assert not task_summary.is_success
        assert "timeout" in str(task_summary.exceptions).lower()

        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        assert task_summary.attributes["task.id"] == "stuck-function"
        assert task_summary.attributes["task.timeout_s"] == 1.0
