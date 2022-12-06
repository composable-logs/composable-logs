import glob
from pathlib import Path
from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.tasks.task_opentelemetry_logging import SerializedData
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_task_span_parser import get_pipeline_iterators
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
    get_duration_s,
    read_key,
)
from otel_output_parser.cli_pynb_log_parser import (
    write_spans_to_output_directory_structure,
    make_mermaid_gantt_inputfile,
    make_mermaid_dag_inputfile,
)

from .nb_test_helpers import assert_no_exceptions, make_test_nb_task, TEST_NOTEBOOK_PATH


def test__jupytext_notebook_task__otel_logging_from_notebook(tmp_path: Path):
    def get_test_spans():
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

    def validate_spans(spans: Spans):
        assert_no_exceptions(spans)

        jupytext_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "task.task_type"], "jupytext")
            #
            .filter(["status", "status_code"], "OK")
        )

        artefacts_span = one(
            spans.filter(["name"], "artefact")
            #
            .filter(["attributes", "name"], "README.md")
            #
            .filter(["status", "status_code"], "OK")
        )
        assert artefacts_span["attributes"]["content_encoded"] == "foobar123"
        assert artefacts_span["attributes"]["encoding"] == "utf-8"

        # artefacts logged from notebook are logged as subspans under the notebook span
        assert spans.contains_path(jupytext_span, artefacts_span)

        def check_named_value(key, value):
            value_span = one(
                spans.filter(["name"], "named-value")
                #
                .filter(["attributes", "name"], key)
                #
                .filter(["status", "status_code"], "OK")
            )
            assert spans.contains_path(jupytext_span, value_span)
            assert (
                SerializedData(
                    type=value_span["attributes"]["type"],
                    encoding=value_span["attributes"]["encoding"],
                    encoded_content=value_span["attributes"]["content_encoded"],
                ).decode()
                == value
            )

        # check values logged with general key-value logger
        check_named_value("value_str_a", "a")
        check_named_value("value_float_1_23", 1.23)
        check_named_value("value_list_1_2_null", [1, 2, None])
        check_named_value("value_dict", {"a": 123, "b": "foo"})
        check_named_value("value_list_nested", [1, [2, None, []]])

        # check values logged with typed-loggers
        check_named_value("boolean_true", True)
        check_named_value("int_1", 1)
        check_named_value("float_1p23", 1.23)
        check_named_value("string_abc", "abc")

    def validate_parsed_spans(spans: Spans):
        _, task_it = get_pipeline_iterators(spans)

        for _, run_it in [one(task_it)]:  # type: ignore
            for _, artefact_it in [one(run_it)]:  # type: ignore
                artefacts = {a["name"]: a for a in artefact_it}

                assert artefacts["README.md"] == {
                    "name": "README.md",
                    "type": "utf-8",
                    "content": "foobar123",
                }
                assert artefacts["class_a/binary.bin"] == {
                    "name": "class_a/binary.bin",
                    "type": "bytes",
                    "content": bytes(range(256)),
                }

                assert len(artefacts) == 3

    def validate_cli_tool(spans: Spans, output_path: Path):
        # check: rendering Mermaid input file contents does not crash
        assert len(make_mermaid_dag_inputfile(spans, generate_links=False)) > 10
        assert len(make_mermaid_gantt_inputfile(spans)) > 10

        write_spans_to_output_directory_structure(spans, output_path)

        files = glob.glob(f"{output_path}/**/*", recursive=True)
        filenames = [Path(f).name for f in files if Path(f).is_file()]

        assert set(filenames) == {
            # --- root of output directory ---
            "pipeline.json",
            # --- one task in pipeline run ---
            "task.json",
            # --- files for single run of task ---
            "notebook.ipynb",
            "notebook.html",
            "run.json",
            "binary.bin",
            "README.md",
        }

    spans = get_test_spans()
    validate_spans(spans)
    validate_parsed_spans(spans)
    validate_cli_tool(spans, tmp_path)
