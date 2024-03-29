import pytest

# -
from composable_logs.helpers import one
from composable_logs.opentelemetry_task_span_parser import parse_spans
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.notebooks_helpers import JupytextNotebookContent
from composable_logs.tasks.tasks import make_jupytext_task
from composable_logs.wrappers import run_dag

# -
from .nb_test_helpers import get_test_jupytext_nb

TASK_PARAMETERS = {
    "workflow.foo": "bar",
    "task.variable_a": "task-value",
}
TEST_NOTEBOOK: JupytextNotebookContent = get_test_jupytext_nb("notebook_ok.py")


@pytest.fixture(scope="module")
def spans_once() -> Spans:
    # Return spans for running DAG with one node where notebook is executed

    with SpanRecorder() as rec:
        run_dag(
            make_jupytext_task(
                notebook=TEST_NOTEBOOK,
                parameters=TASK_PARAMETERS,
                timeout_s=None,
            )()
        )

    return rec.spans


def test__jupytext__ok_notebook__parse_spans(spans_once: Spans):
    workflow_summary = parse_spans(spans_once)

    # assert there is one successful task
    task_summary = one(workflow_summary.task_runs)
    assert task_summary.is_success()

    assert task_summary.attributes == {
        **TASK_PARAMETERS,
        "task.num_cpus": 1,
        "task.id": "notebook_ok",
        "task.type": "jupytext",
        "task.timeout_s": -1,
        # None converted into -1 since OpenTelemetry attributes should be non-null
    }

    # Check properties of artifact with the evaluated notebook
    assert set([a.name for a in task_summary.logged_artifacts]) == {
        "notebook.ipynb",
        "notebook.html",
    }

    def validate(artifact):
        assert isinstance(artifact.content, str)
        assert len(artifact.content) > 1000
        assert artifact.type == "utf-8"

        # Notebook prints:
        #  - 1 + 12 + 123 + 1234 + 12345
        #  - 'variable_a={P["task.variable_a"]}'
        # Check that these evaluates:
        assert "variable_a=task-value" in artifact.content
        assert str(1 + 12 + 123 + 1234 + 12345) in artifact.content

    for name in ["notebook.ipynb", "notebook.html"]:
        validate(one(a for a in task_summary.logged_artifacts if a.name == name))

    assert len(workflow_summary.task_dependencies) == 0


@pytest.fixture(scope="module")
def spans_run_twice() -> Spans:
    # Return spans for running DAG with two nodes
    #
    #      "notebook_ok"    --->       "notebook_ok"
    #

    with SpanRecorder() as rec:

        nb = make_jupytext_task(notebook=TEST_NOTEBOOK, parameters=TASK_PARAMETERS)

        node1 = nb()
        node2 = nb(node1)

        run_dag(node2)

    return rec.spans


def test__jupytext__ok_notebook__run_twice(spans_run_twice: Spans):
    workflow_summary = parse_spans(spans_run_twice)

    # assert there is one successful task
    assert len(workflow_summary.task_runs) == 2

    for task_summary in workflow_summary.task_runs:
        assert task_summary.is_success()
        assert task_summary.task_id == "notebook_ok"

    assert len(workflow_summary.task_dependencies) == 1
