from pathlib import Path

#
import pytest

#
from pynb_dag_runner.helpers import del_key, one
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder
from pynb_dag_runner.notebooks_helpers import JupytextNotebookContent
from pynb_dag_runner.tasks.tasks import make_jupytext_task
from pynb_dag_runner.wrappers import run_dag

# -
from .nb_test_helpers import get_test_jupytext_nb

#

TASK_PARAMETERS = {"pipeline.foo": "bar", "task.variable_a": "task-value"}
TEST_NOTEBOOK = get_test_jupytext_nb("notebook_ok.py")


@pytest.fixture(scope="module")
def spans_once() -> Spans:
    # Return spans for running DAG with one node where notebook is executed

    with SpanRecorder() as rec:
        run_dag(
            make_jupytext_task(notebook=TEST_NOTEBOOK, parameters=TASK_PARAMETERS)()
        )

    return rec.spans


def test__jupytext__ok_notebook__parse_spans(spans_once: Spans):
    pipeline_summary = parse_spans(spans_once)

    # assert there is one successful task
    task_summary = one(pipeline_summary.task_runs)
    assert task_summary.is_success()

    # assert attributes are logged
    assert task_summary.attributes["task.notebook"] == str(TEST_NOTEBOOK.filepath)

    assert del_key(task_summary.attributes, "task.notebook") == {
        **TASK_PARAMETERS,
        "task.num_cpus": 1,
        "task.task_id": str(TEST_NOTEBOOK.filepath),
        "task.task_type": "jupytext",
        # "task.timeout_s": 10.0,
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

    assert len(pipeline_summary.task_dependencies) == 0


@pytest.fixture(scope="module")
def spans_run_twice() -> Spans:
    # Return spans for running DAG with two nodes
    #
    #      "notebook_ok.py"    --->       "notebook_ok.py"
    #

    with SpanRecorder() as rec:

        nb = make_jupytext_task(notebook=TEST_NOTEBOOK, parameters=TASK_PARAMETERS)

        node1 = nb()
        node2 = nb(node1)

        run_dag(node2)

    return rec.spans


def test__jupytext__ok_notebook__run_twice(spans_run_twice: Spans):
    pipeline_summary = parse_spans(spans_run_twice)

    # assert there is one successful task
    assert len(pipeline_summary.task_runs) == 2

    for task_summary in pipeline_summary.task_runs:
        assert task_summary.is_success()
        assert task_summary.task_id == str(TEST_NOTEBOOK.filepath)

    assert len(pipeline_summary.task_dependencies) == 1
