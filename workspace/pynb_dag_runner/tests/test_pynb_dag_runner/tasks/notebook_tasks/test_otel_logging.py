import glob
from pathlib import Path
from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_task_span_parser import (
    parse_spans,
    LoggedValueContent,
)
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)
from otel_output_parser.cli_pynb_log_parser import (
    write_spans_to_output_directory_structure,
    make_mermaid_gantt_inputfile,
    make_mermaid_dag_inputfile,
)

from .nb_test_helpers import make_test_nb_task


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    """
    Pytest fixture to return Spans after running notebook_otel_logging.py notebook
    """
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_otel_logging.py",
            max_nr_retries=1,
            parameters={
                "task.variable_a": "task-value",
                "pipeline.pipeline_run_id": "12345",
            },
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__otel_logging_from_notebook__validate_parsed_spans_new(spans: Spans):
    pipeline_summary = parse_spans(spans)

    assert pipeline_summary.is_success()

    task_summary = one(pipeline_summary.task_runs)
    assert task_summary.is_success

    # Check: artifact logged from the evaluated notebook
    artifacts = task_summary.logged_artifacts
    assert artifacts.keys() == {"README.md", "class_a/binary.bin", "notebook.ipynb"}
    assert artifacts["README.md"].type == "utf-8"
    assert artifacts["README.md"].content == "foobar123"

    assert artifacts["notebook.ipynb"].type == "utf-8"
    assert len(artifacts["notebook.ipynb"].content) > 1000

    assert artifacts["class_a/binary.bin"].type == "bytes"
    assert artifacts["class_a/binary.bin"].content == bytes(range(256))

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

    assert set(filenames) == {
        # --- root of output directory ---
        "pipeline.json",
        # --- one task in pipeline run ---
        "task.json",
        # --- files for single run of task ---
        "run.json",
        # artifacts are written to disk
        "notebook.ipynb",
        "notebook.html",
        "binary.bin",
        "README.md",
        # notebooks are converted into html
        "notebook.html",
    }
