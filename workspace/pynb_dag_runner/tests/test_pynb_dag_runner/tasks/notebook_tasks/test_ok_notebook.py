from functools import lru_cache
from typing import Any, Dict

#
import pytest

#
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import del_key, one
from pynb_dag_runner.opentelemetry_task_span_parser import get_pipeline_iterators
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans, Artifact
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder, read_key

from .nb_test_helpers import assert_no_exceptions, make_test_nb_task, TEST_NOTEBOOK_PATH

NOTEBOOK_PATH = str(TEST_NOTEBOOK_PATH / "notebook_ok.py")
TASK_PARAMETERS = {"pipeline.foo": "bar", "task.variable_a": "task-value"}


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_ok.py",
            max_nr_retries=1,
            parameters=TASK_PARAMETERS,
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__ok_notebook__validate_spans(spans: Spans):
    assert_no_exceptions(spans)

    jupytext_span = one(
        spans.filter(["name"], "execute-task")
        #
        .filter(["attributes", "task.task_type"], "jupytext")
    )
    assert jupytext_span["status"] == {"status_code": "OK"}

    assert read_key(jupytext_span, ["attributes", "task.notebook"]) == NOTEBOOK_PATH

    artefact_span = one(spans.filter(["name"], "artefact"))

    # see notebook for motivation behind these string-tests
    for content in [str(1 + 12 + 123), "variable_a=task-value"]:
        assert content in artefact_span["attributes"]["content_encoded"]

    spans.contains_path(jupytext_span, artefact_span)


def test__jupytext__ok_notebook__validate_parsed_spans_1(spans: Spans):
    # test parsed spans:
    #   - pipeline, task, run attributes
    #   - notebook is logged as artifact
    pipeline_dict, task_it = get_pipeline_iterators(spans)
    expected_pipeline_attributes = {"pipeline.foo": "bar"}
    assert expected_pipeline_attributes == pipeline_dict["attributes"]

    for task_dict, run_it in [one(task_it)]:
        expected_task_attributes = {
            **expected_pipeline_attributes,
            "task.variable_a": "task-value",
            "task.max_nr_retries": 1,
            "task.notebook": str(TEST_NOTEBOOK_PATH / "notebook_ok.py"),
            "task.num_cpus": 1,
            "task.task_type": "jupytext",
            "task.timeout_s": 10.0,
        }
        assert expected_task_attributes == task_dict["attributes"]

        for run_dict, run_artefact_it in [one(run_it)]:  # type: ignore
            expected_run_attributes: Dict[str, Any] = {
                **expected_pipeline_attributes,
                **expected_task_attributes,
                "run.retry_nr": 0,
            }
            assert expected_run_attributes == run_dict["attributes"]

            artefacts = {a["name"]: a for a in run_artefact_it}
            assert set(artefacts.keys()) == {"notebook.ipynb"}

            assert artefacts["notebook.ipynb"]["type"] == "utf-8"
            assert len(artefacts["notebook.ipynb"]["content"]) > 1000


def test__jupytext__ok_notebook__validate_parsed_spans_2(spans: Spans):
    pipeline_dict, task_it = get_pipeline_iterators(spans)

    common_keys = {
        "span_id",
        "start_time",
        "end_time",
        "duration_s",
        "status",
        "attributes",
    }

    assert pipeline_dict.keys() == {"attributes", "task_dependencies"}
    assert pipeline_dict["task_dependencies"] == []
    expected_pipeline_attributes = {"pipeline.foo": "bar"}
    assert pipeline_dict["attributes"] == expected_pipeline_attributes

    for task_dict, run_it in [one(task_it)]:  # type: ignore
        assert task_dict.keys() == common_keys  # type: ignore
        for k in ["span_id", "start_time", "end_time"]:
            assert isinstance(task_dict[k], str)  # type: ignore
        assert isinstance(task_dict["duration_s"], float)  # type: ignore

        assert task_dict["status"] == {"status_code": "OK"}  # type: ignore

        expected_task_attributes: Dict[str, Any] = {
            "task.variable_a": "task-value",
            "task.max_nr_retries": 1,
            "task.notebook": NOTEBOOK_PATH,
            "task.num_cpus": 1,
            "task.task_type": "jupytext",
            "task.timeout_s": 10.0,
        }
        assert {
            **expected_pipeline_attributes,
            **expected_task_attributes,
        } == task_dict["attributes"]

        for run_dict, artefact_it in [one(run_it)]:  # type: ignore
            assert run_dict.keys() == common_keys | {"logged_values"}
            expected_run_attributes: Dict[str, Any] = {
                "run.retry_nr": 0,
                **expected_pipeline_attributes,
                **expected_task_attributes,
            }
            assert expected_run_attributes == run_dict["attributes"]

            # check that one notebooks artefact should be logged
            for artefact_dict in [one(artefact_it)]:
                assert artefact_dict.keys() == {
                    "name",
                    "type",
                    "content",
                }


def test__jupytext__ok_notebook__parse_spans(spans: Spans):
    pipeline_summary = parse_spans(spans)

    # assert there is one successful task
    task_summary = one(pipeline_summary.task_runs)
    assert task_summary.is_success

    # assert attributes are logged
    assert task_summary.attributes["task.notebook"] == NOTEBOOK_PATH

    assert del_key(task_summary.attributes, "task.notebook") == {
        **TASK_PARAMETERS,
        "task.max_nr_retries": 1,
        "task.num_cpus": 1,
        "task.task_type": "jupytext",
        "task.timeout_s": 10.0,
    }

    # assert content of artifact
    artifact = one(task_summary.logged_artifacts)
    assert isinstance(artifact.content, str)

    assert str(1 + 12 + 123) in artifact.content
    assert artifact.type == "utf-8"
    assert artifact.name == "notebook.ipynb"
