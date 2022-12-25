import pytest

#
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import del_key, one
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder

#
from .nb_test_helpers import make_test_nb_task, TEST_NOTEBOOK_PATH

NOTEBOOK_PATH = str(TEST_NOTEBOOK_PATH / "notebook_ok.py")
TASK_PARAMETERS = {"pipeline.foo": "bar", "task.variable_a": "task-value"}


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_ok.py",
            max_nr_retries=1,
            parameters=TASK_PARAMETERS,
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__ok_notebook__parse_spans(spans: Spans):
    pipeline_summary = parse_spans(spans)

    # assert there is one successful task
    task_summary = one(pipeline_summary.task_runs)
    assert task_summary.is_success()

    # assert attributes are logged
    assert task_summary.attributes["task.notebook"] == NOTEBOOK_PATH

    assert del_key(task_summary.attributes, "task.notebook") == {
        **TASK_PARAMETERS,
        "task.max_nr_retries": 1,
        "task.num_cpus": 1,
        "task.task_type": "jupytext",
        "task.timeout_s": 10.0,
    }

    # Check properties of artifact with the evaluated notebook
    assert task_summary.logged_artifacts.keys() == {"notebook.ipynb", "notebook.html"}

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

    validate(task_summary.logged_artifacts["notebook.ipynb"])
    validate(task_summary.logged_artifacts["notebook.html"])
