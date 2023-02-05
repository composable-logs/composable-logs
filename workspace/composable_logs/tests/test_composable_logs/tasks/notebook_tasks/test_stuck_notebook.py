import pytest

# -
from composable_logs.helpers import one
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.notebooks_helpers import JupytextNotebookContent
from composable_logs.tasks.tasks import make_jupytext_task
from composable_logs.wrappers import run_dag
from composable_logs.opentelemetry_task_span_parser import parse_spans

# -
from .nb_test_helpers import get_test_jupytext_nb


TASK_PARAMETERS = {
    "workflow.foo": "bar",
    "task.variable_a": "task-value",
}
TEST_NOTEBOOK: JupytextNotebookContent = get_test_jupytext_nb("notebook_stuck.py")
TASK_TIMEOUT_S = 1.0


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:
        run_dag(
            make_jupytext_task(
                notebook=TEST_NOTEBOOK,
                parameters=TASK_PARAMETERS,
                timeout_s=TASK_TIMEOUT_S,
            )()
        )

    return rec.spans


def test__jupytext__stuck_notebook__validate_spans(spans: Spans):
    workflow_summary = parse_spans(spans)
    assert workflow_summary.is_failure()

    for task_summary in [one(workflow_summary.task_runs)]:  # type: ignore
        assert task_summary.is_failure()
        assert "timeout" in str(one(task_summary.exceptions)).lower()

        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        assert task_summary.attributes == {
            "task.id": "notebook_stuck",
            "task.type": "jupytext",
            "task.num_cpus": 1,
            "task.timeout_s": TASK_TIMEOUT_S,
            **TASK_PARAMETERS,
        }

        # TODO: ideally check here that logged artifacts and values are still captured
        # even if the task is killed early.

        assert task_summary.timing.get_duration_s() > TASK_TIMEOUT_S

    assert len(workflow_summary.task_dependencies) == 0
