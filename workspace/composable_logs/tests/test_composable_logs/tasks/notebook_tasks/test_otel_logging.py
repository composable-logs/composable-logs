import glob
from pathlib import Path

# -
import pytest

# -
from composable_logs.helpers import one
from composable_logs.opentelemetry_task_span_parser import (
    parse_spans,
    LoggedValueContent,
)
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from otel_output_parser.cli_pynb_log_parser import (
    write_spans_to_output_directory_structure,
    make_mermaid_gantt_inputfile,
    make_mermaid_dag_inputfile,
)

from composable_logs.notebooks_helpers import JupytextNotebookContent
from composable_logs.tasks.tasks import make_jupytext_task
from composable_logs.wrappers import run_dag

# -
from .nb_test_helpers import get_test_jupytext_nb

# -

TASK_PARAMETERS = {
    "workflow.foo": "bar",
    "task.variable_a": "task-value",
}
TEST_NOTEBOOK: JupytextNotebookContent = get_test_jupytext_nb(
    "notebook_otel_logging.py"
)

# -
@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:
        run_dag(
            make_jupytext_task(
                notebook=TEST_NOTEBOOK, parameters=TASK_PARAMETERS, timeout_s=10.0
            )()
        )

    return rec.spans


def test__jupytext__otel_logging_from_notebook__validate_parsed_spans_new(spans: Spans):
    workflow_summary = parse_spans(spans)

    # assert workflow_summary.is_success()

    task_summary = one(workflow_summary.task_runs)
    # assert task_summary.is_success()

    # Check: artifact logged from the evaluated notebook
    artifacts = task_summary.logged_artifacts

    assert set(artifact.name for artifact in artifacts) == {
        "README.md",
        "class_a/binary.bin",
        "notebook.ipynb",
        "notebook.html",
    }

    def lookup_artifact(artifact_name):
        return [one(x for x in artifacts if x.name == artifact_name)]

    for artifact in lookup_artifact("README.md"):
        assert artifact.type == "utf-8"
        assert artifact.content == "foobar123"

    for artifact in lookup_artifact("notebook.ipynb"):
        assert artifact.type == "utf-8"
        assert len(artifact.content) > 1000

    for artifact in lookup_artifact("class_a/binary.bin"):
        assert artifact.type == "bytes"
        assert artifact.content == bytes(range(256))

    # Check: logged values from notebook
    logged_values = task_summary.logged_values
    assert logged_values.keys() == {
        "value_str_a",
        "value_float_1_23",
        "value_list_1_2_null",
        "value_dict",
        "value_list_nested",
        "boolean_true",
        "int_1",
        "float_1p23",
        "string_abc",
    }

    # -- logging of generic json values ---
    assert logged_values["value_str_a"] == LoggedValueContent(type="utf-8", content="a")
    assert logged_values["value_float_1_23"] == LoggedValueContent(
        type="float", content=1.23
    )
    assert logged_values["value_list_1_2_null"] == LoggedValueContent(
        type="json", content=[1, 2, None]
    )
    assert logged_values["value_dict"] == LoggedValueContent(
        type="json", content={"a": 123, "b": "foo"}
    )

    assert logged_values["value_list_nested"] == LoggedValueContent(
        type="json", content=[1, [2, None, []]]
    )

    # -- logging of typed values ---
    assert logged_values["boolean_true"] == LoggedValueContent(
        type="bool", content=True
    )
    assert logged_values["int_1"] == LoggedValueContent(type="int", content=1)
    assert logged_values["float_1p23"] == LoggedValueContent(type="float", content=1.23)
    assert logged_values["string_abc"] == LoggedValueContent(
        type="utf-8", content="abc"
    )


def test__jupytext__otel_logging_from_notebook__validate_cli_tool(
    spans: Spans, tmp_path: Path
):
    # check: rendering Mermaid input file contents does not crash
    assert len(make_mermaid_dag_inputfile(spans, generate_links=False)) > 10
    assert len(make_mermaid_gantt_inputfile(spans)) > 10

    write_spans_to_output_directory_structure(spans, tmp_path)

    files = glob.glob(f"{tmp_path}/**/*", recursive=True)
    filenames = [Path(f).name for f in files if Path(f).is_file()]

    # "run-time-metadata.json" is written twice
    assert len(set(filenames)) == len(filenames) - 1

    assert set(filenames) == {
        # --- one task is executed in this workflow run ---
        "run-time-metadata.json",
        # --- files for single run of task ---
        "run-time-metadata.json",  # same as above, not ideal
        # artifacts are written to disk
        "notebook.ipynb",
        "notebook.html",
        "binary.bin",
        "README.md",
        # notebooks are converted into html
        "notebook.html",
    }
