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
    SpanDict,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_task_artifact_iterators,
)


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
    _, task_run_it = get_pipeline_task_artifact_iterators(spans)

    for task_run_summary, artefacts in [one(task_run_it)]:  # type: ignore
        assert len(artefacts) == 0

        assert not task_run_summary.is_success
        assert len(task_run_summary.exceptions) == 1
        assert "timeout" in str(task_run_summary.exceptions).lower()

        assert task_run_summary.attributes["task.id"] == "stuck-function"
        assert task_run_summary.attributes["task.timeout_s"] == 1.0
