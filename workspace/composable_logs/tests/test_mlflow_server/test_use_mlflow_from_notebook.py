import pytest

# -
from composable_logs.helpers import one
from composable_logs.opentelemetry_task_span_parser import parse_spans
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.notebooks_helpers import JupytextNotebookContent
from composable_logs.tasks.tasks import make_jupytext_task
from composable_logs.wrappers import run_dag

# -
from composable_logs.notebooks_helpers import JupytextNotebookContent


TEST_JUPYTEXT_NOTEBOOK = """# %%
# %%
P = {"task.variable_a": "value-used-during-interactive-development"}
# %% tags=["parameters"]
# ---- During automated runs parameters will be injected in this cell ---
# %%
# -----------------------------------------------------------------------
# %%
# %%
import mlflow

# -
from composable_logs.tasks.task_opentelemetry_logging import get_task_context
# %%
ctx = get_task_context(P, use_mlflow_for_logging=True)

# MLFlow server is now running in the Ray cluster, and the below will connect to that
mlflow.log_param("foo", "abc")

# %%

"""


@pytest.fixture(scope="module")
def notebook_spans() -> Spans:
    with SpanRecorder() as rec:
        run_dag(
            make_jupytext_task(
                notebook=JupytextNotebookContent(
                    filepath="foo.py", content=TEST_JUPYTEXT_NOTEBOOK
                ),
                parameters={},
                timeout_s=None,
            )()
        )

    return rec.spans


def test_mlflow_server_is_automatically_started_from_jupytext_tasks(
    notebook_spans: Spans,
):
    workflow_summary = parse_spans(notebook_spans)
    assert workflow_summary.is_success()

    for task_run_summary in [one(workflow_summary.task_runs)]:
        assert len(task_run_summary.logged_values.keys()) == 1
        assert task_run_summary.logged_values["foo"].content == "abc"
