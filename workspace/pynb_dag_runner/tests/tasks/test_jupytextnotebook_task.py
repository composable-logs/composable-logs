import datetime, json
from pathlib import Path

#
import pytest

#
from pynb_dag_runner.tasks.tasks import make_jupytext_task_ot
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_task_span_parser import get_pipeline_iterators
from pynb_dag_runner.notebooks_helpers import JupytextNotebook
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
    get_duration_s,
    read_key,
)


def assert_no_exceptions(spans: Spans):
    exceptions = spans.exception_events()

    if len(exceptions) == 0:
        print("***** No exceptions ******")
    else:
        print(f"***** {len(exceptions)} exceptions in total ******")
        for e in exceptions:
            print(100 * "=")
            print(json.dumps(e, indent=2))


def isotimestamp_normalized():
    """
    Return ISO timestamp modified (by replacing : with _) so that it can be used
    as part of a directory or file name.

    Eg "YYYY-MM-DDTHH-MM-SS.ffffff+00-00"

    This is useful to generate output directories that are guaranteed to not exist.
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")


def make_test_nb_task(
    nb_name: str, max_nr_retries: int, parameters={}, timeout_s: float = 10.0
):
    nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"
    return make_jupytext_task_ot(
        notebook=JupytextNotebook(nb_path / nb_name),
        tmp_dir=nb_path,
        timeout_s=timeout_s,
        max_nr_retries=max_nr_retries,
        parameters=parameters,
    )


def test__jupytext_notebook_task__run_ok_notebook():
    NB_PATH = str((Path(__file__).parent)) + "/jupytext_test_notebooks/notebook_ok.py"

    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_ok.py",
                max_nr_retries=2,
                parameters={"pipeline.foo": "bar", "task.variable_a": "task-value"},
            )
            _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.exception_events()) == 0

        jupytext_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "task.task_type"], "jupytext")
        )
        assert jupytext_span["status"] == {"status_code": "OK"}

        assert read_key(jupytext_span, ["attributes", "task.notebook"]) == NB_PATH

        artefact_span = one(spans.filter(["name"], "artefact"))

        # see notebook for motivation behind these string-tests
        for content in [str(1 + 12 + 123), "variable_a=task-value"]:
            assert content in artefact_span["attributes"]["content"]

        spans.contains_path(jupytext_span, artefact_span)

    def validate_parsed_spans(spans: Spans):
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

        for task_dict, run_it in [one(task_it)]:
            assert task_dict.keys() == common_keys
            for k in ["span_id", "start_time", "end_time"]:
                assert isinstance(task_dict[k], str)
            assert isinstance(task_dict["duration_s"], float)

            assert task_dict["status"] == {"status_code": "OK"}

            expected_task_attributes = {
                "task.variable_a": "task-value",
                "task.max_nr_retries": 2,
                "task.notebook": NB_PATH,
                "task.num_cpus": 1,
                "task.task_type": "jupytext",
                "task.timeout_s": 10.0,
            }
            assert task_dict["attributes"] == {
                **expected_pipeline_attributes,
                **expected_task_attributes,
            }

            for run_dict, artefact_it in [one(run_it)]:
                assert run_dict.keys() == common_keys | {"logged_values"}
                assert run_dict["attributes"] == {
                    "run.retry_nr": 0,
                    "task.num_cpus": 1,
                    "task.timeout_s": 10.0,
                    # TODO: all pipeline and task attributes are not listed here
                }

                # check that one notebooks artefact should be logged
                for artefact_dict in [one(artefact_it)]:
                    assert artefact_dict.keys() == {"name", "encoding", "content"}

    spans = get_test_spans()
    validate_spans(spans)
    validate_parsed_spans(spans)


def test__jupytext_notebook_task__always_fail():
    N_retries = 3

    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_always_fail.py",
                max_nr_retries=N_retries,
                parameters={"task.injected_parameter": 19238},
            )
            _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.exception_events()) > 0
        top_task_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "task.task_type"], "jupytext")
        )
        assert top_task_span["status"] == {
            "status_code": "ERROR",
            "description": "Remote function call failed",
        }

        assert (
            read_key(top_task_span, ["attributes", "task.notebook"])
            == str((Path(__file__).parent))
            + "/jupytext_test_notebooks/notebook_always_fail.py"
        )

        top_retry_span = one(spans.filter(["name"], "retry-wrapper"))
        assert spans.contains_path(top_task_span, top_retry_span)
        assert (
            read_key(top_retry_span, ["attributes", "task.max_nr_retries"]) == N_retries
        )

        retry_call_spans = spans.filter(["name"], "retry-call")
        assert len(retry_call_spans) == N_retries

        for retry_span in retry_call_spans:
            retry_spans: Spans = spans.bound_under(retry_span)
            assert len(retry_spans.exception_events()) == 1

            artefact_span = one(retry_spans.filter(["name"], "artefact"))
            assert "task.injected_parameter" in artefact_span["attributes"]["content"]

            spans.contains_path(
                top_task_span, top_retry_span, retry_span, artefact_span
            )

    validate_spans(get_test_spans())


@pytest.mark.parametrize("N_retries", [2, 10])
def test__jupytext_notebook_task__exception_throwing_notebook(N_retries):
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_exception.py",
                max_nr_retries=N_retries,
                parameters={"task.variable_a": "task-value"},
            )
            _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

        return rec.spans

    # notebook will fail on first three runs. Depending on number of retries
    # determine which run:s are success/failed.
    def ok_indices():
        if N_retries == 2:
            return []
        else:
            return [3]

    def failed_indices():
        if N_retries == 2:
            return [0, 1]
        else:
            return [0, 1, 2]

    def nr_notebook_executions():
        if N_retries == 2:
            return 2
        else:
            return 4

    def validate_spans(spans: Spans):
        top_task_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "task.task_type"], "jupytext")
        )

        top_retry_span = one(spans.filter(["name"], "retry-wrapper"))
        assert spans.contains_path(top_task_span, top_retry_span)
        assert (
            read_key(top_retry_span, ["attributes", "task.max_nr_retries"]) == N_retries
        )

        retry_call_spans = spans.filter(["name"], "retry-call")
        assert len(retry_call_spans) == nr_notebook_executions()

        if len(ok_indices()) > 0:
            assert top_task_span["status"] == {"status_code": "OK"}
        else:
            assert top_task_span["status"] == {
                "status_code": "ERROR",
                "description": "Remote function call failed",
            }
        run_spans = spans.filter(["name"], "retry-call").sort_by_start_time()
        assert len(run_spans) == len(ok_indices()) + len(failed_indices())

        for idx in ok_indices():
            assert run_spans[idx]["status"] == {"status_code": "OK"}

        for idx in failed_indices():
            failed_run_span = run_spans[idx]

            exception = one(spans.bound_inclusive(failed_run_span).exception_events())[
                "attributes"
            ]
            assert exception["exception.type"] == "Exception"
            assert "Thrown from notebook!" in exception["exception.message"]

            assert run_spans[idx]["status"] == {
                "status_code": "ERROR",
                "description": "Run failed",
            }

        for idx in failed_indices() + ok_indices():
            # for both successful and failed runs, a partially evaluated notebook
            # should have been logged as an artefact.
            artefact_span = one(
                spans.bound_under(run_spans[idx]).filter(["name"], "artefact")
            )
            assert artefact_span["attributes"]["name"] == "notebook.ipynb"
            assert str(1 + 12 + 123) in artefact_span["attributes"]["content"]

    validate_spans(get_test_spans())


def test__jupytext_notebook_task__stuck_notebook():
    """
    Currently, timeout canceling is done on Ray level, but error handling and
    recovery is done only within the Python process (using try .. catch).
    Therefore, timeout canceled tasks can not currently do proper error handling.
    """

    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_stuck.py",
                max_nr_retries=1,
                timeout_s=10.0,
                parameters={},
            )
            _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

        return rec.spans

    def validate_spans(spans: Spans):
        top_task_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "task.task_type"], "jupytext")
        )

        assert top_task_span["status"] == {
            "description": "Remote function call failed",
            "status_code": "ERROR",
        }

        timeout_guard_span = one(spans.filter(["name"], "timeout-guard"))
        assert timeout_guard_span["status"] == {
            "status_code": "ERROR",
            "description": "Timeout",
        }

        spans.contains_path(top_task_span, timeout_guard_span)

        assert get_duration_s(top_task_span) > get_duration_s(timeout_guard_span) > 10.0

        assert len(spans.exception_events()) == 1

        # notebook evaluation never finishes, and is cancled by Ray. Therefore no
        # artefact ipynb content is logged
        assert len(spans.filter(["name"], "artefact")) == 0

    validate_spans(get_test_spans())


def test__jupytext_notebook_task__otel_logging_from_notebook():
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_otel_logging.py",
                max_nr_retries=1,
                parameters={"task.variable_a": "task-value"},
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
            .filter(["attributes", "name"], "from_notebook.txt")
            #
            .filter(["status", "status_code"], "OK")
        )
        assert artefacts_span["attributes"]["content"] == "foobar123"
        assert artefacts_span["attributes"]["encoding"] == "text/utf8"

        # artefacts logged from notebook are logged as subspans under the notebook span
        assert spans.contains_path(jupytext_span, artefacts_span)

        def check_named_value(key, value, encoding):
            value_span = one(
                spans.filter(["name"], "named-value")
                #
                .filter(["attributes", "name"], key)
                #
                .filter(["status", "status_code"], "OK")
            )
            assert value_span["attributes"]["value"] == json.dumps(value)

            assert spans.contains_path(jupytext_span, value_span)

        # check values logged with general key-value logger
        check_named_value("value_str_a", "a", "json")
        check_named_value("value_null", None, "json")
        check_named_value("value_float_1_23", 1.23, "json")
        check_named_value("value_list_1_2_null", [1, 2, None], "json")
        check_named_value("value_dict", {"a": 123, "b": "foo"}, "json")
        check_named_value("value_list_nested", [1, [2, None, []]], "json")

        # check values logged with typed-loggers
        check_named_value("boolean_true", True, "json/bool")
        check_named_value("int_1", 1, "json/int")
        check_named_value("float_1p23", 1.23, "json/float")
        check_named_value("string_abc", "abc", "json/string")

    validate_spans(get_test_spans())
