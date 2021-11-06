import random, os, datetime
from pathlib import Path
from typing import Any, Dict

#
import pytest

#
from pynb_dag_runner.tasks.tasks import JupytextNotebookTask, make_jupytext_task

from pynb_dag_runner.core.dag_runner import run_tasks, TaskDependencies
from pynb_dag_runner.helpers import one, flatten, read_json
from pynb_dag_runner.notebooks_helpers import JupytextNotebook
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)

# TODO: all the below tests should run multiple times in stress tests
# See, https://github.com/pynb-dag-runner/pynb-dag-runner/pull/5


def isotimestamp_normalized():
    """
    Return ISO timestamp modified (by replacing : with _) so that it can be used
    as part of a directory or file name.

    Eg "YYYY-MM-DDTHH-MM-SS.ffffff+00-00"

    This is useful to generate output directories that are guaranteed to not exist.
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")


def test_jupytext_run_ok_notebook():
    def get_test_spans():
        with SpanRecorder() as rec:
            dependencies = TaskDependencies()

            nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"
            jupytext_task = make_jupytext_task(
                notebook=JupytextNotebook(nb_path / "notebook_ok.py"),
                task_id="123",
                tmp_dir=nb_path,
                timeout_s=5,
                n_max_retries=1,
                task_parameters={"task.variable_a": "task-value"},
            )

            run_tasks([jupytext_task], dependencies)

        return rec.spans

    def validate_spans(spans: Spans):
        py_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "python"
            )
        )
        assert py_span["status"] == {"status_code": "OK"}

        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        spans.contains_path(jupytext_span, py_span)

        assert jupytext_span["status"] == {"status_code": "OK"}
        for content in ["<html>", str(1 + 12 + 123), "variable_a=task-value"]:
            assert content in jupytext_span["attributes"]["notebook_html"]

    validate_spans(get_test_spans())


@pytest.mark.parametrize("N_retries", [2, 10])
def test_jupytext_exception_throwing_notebook(N_retries):
    def get_test_spans():
        with SpanRecorder() as rec:
            dependencies = TaskDependencies()

            nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"
            jupytext_task = make_jupytext_task(
                notebook=JupytextNotebook(nb_path / "notebook_exception.py"),
                task_id="123",
                tmp_dir=nb_path,
                timeout_s=5,
                n_max_retries=N_retries,
                task_parameters={},
            )

            run_tasks([jupytext_task], dependencies)

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

    def validate_spans(spans: Spans):
        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        if len(ok_indices()) > 0:
            assert jupytext_span["status"] == {"status_code": "OK"}
        else:
            assert jupytext_span["status"] == {
                "status_code": "ERROR",
                "description": "Jupytext notebook task failed",
            }

        run_spans = spans.filter(["name"], "task-run").sort_by_start_time()
        assert len(run_spans) == len(ok_indices()) + len(failed_indices())

        for idx in ok_indices():
            assert run_spans[idx]["status"] == {"status_code": "OK"}

        for idx in failed_indices():
            failed_run_span = run_spans[idx]

            exception = one(spans.exceptions_in(failed_run_span))["attributes"]
            assert exception["exception.type"] == "PapermillExecutionError"
            assert "Thrown from notebook!" in exception["exception.message"]

            assert run_spans[idx]["status"] == {
                "status_code": "ERROR",
                "description": "Run failed",
            }

        # for both successful and failed runs, there should be (partially evaluated)
        # notebook in html format
        for content in ["<html>", str(1 + 12 + 123)]:
            assert content in jupytext_span["attributes"]["notebook_html"]

    validate_spans(get_test_spans())


def skip_test_mini_jupytext_pipeline(tmp_path: Path):

    nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"

    # tmp_path = nb_path     # uncomment for easier access to runlogs when debugging
    out_path: Path = tmp_path / "out" / isotimestamp_normalized()

    notebook_ok = JupytextNotebookTask(
        notebook=JupytextNotebook(nb_path / "notebook_ok.py"),
        task_id="id=ok",
        get_run_path=lambda _: out_path / "ok",
        parameters={"parameters.task.variable_a": "hello"},
    )

    notebook_stuck = JupytextNotebookTask(
        notebook=JupytextNotebook(nb_path / "notebook_stuck.py"),
        task_id="id=stuck",
        timeout_s=2,
        get_run_path=lambda _: out_path / "stuck",
        parameters={},
    )

    notebook_exception = JupytextNotebookTask(
        notebook=JupytextNotebook(nb_path / "notebook_exception.py"),
        task_id="id=exception",
        get_run_path=lambda _: out_path / "exception",
        parameters={},
    )

    tasks = [notebook_ok, notebook_stuck, notebook_exception]
    random.shuffle(tasks)
    runlog_results = flatten(run_tasks(tasks, TaskDependencies()))  # type: ignore

    # ---- assert properties about output ----

    assert len(runlog_results) == 3

    def get_data(run_directory: Path):
        def option_read_file(filepath: Path, as_json: bool):
            if filepath.is_file():
                if as_json:
                    return read_json(filepath)
                else:
                    return filepath.read_text()
            else:
                return None

        result: Dict[str, Any] = {"other_files": {}}

        for file in os.listdir(run_directory):
            if file.endswith(".html"):
                result["html"] = option_read_file(run_directory / file, as_json=False)
            elif file.endswith(".ipynb") and file.startswith("notebook_"):
                # file is evaluated notebook (and not temp notebook "temp-....ipynb")
                result["ipynb"] = option_read_file(run_directory / file, as_json=False)
            elif file == "runlog.json":
                result["runlog"] = option_read_file(run_directory / file, as_json=True)
            elif file == "_SUCCESS":
                result["_SUCCESS"] = option_read_file(
                    run_directory / file, as_json=False
                )
            else:
                result["other_files"][file] = option_read_file(
                    run_directory / file, as_json=False
                )

        return result

    # ----
    def validate_ok_run_directory(data):
        assert (
            set(["ipynb", "html", "runlog", "_SUCCESS", "other_files"]) == data.keys()
        )
        for expected_string in [str(1 + 12 + 123), "variable_a=hello"]:
            assert expected_string in data["ipynb"]
            assert expected_string in data["html"]

        # runlog content
        assert data["runlog"]["parameters.task.variable_a"] == "hello"
        assert data["runlog"]["parameters.run.run_directory"] == str(out_path / "ok")
        assert data["runlog"]["task_id"] == "id=ok"
        assert data["runlog"]["out.status"] == "SUCCESS"

        #
        assert data["_SUCCESS"] == ""
        assert data["other_files"] == dict()

    validate_ok_run_directory(get_data(out_path / "ok"))

    # ----
    def validate_stuck_notebook_run_directory(data):
        """
        Currently, timeout canceling is done on Ray level, but error handling and
        recovery is done only within the Python process. Therefore, timeout canceled
        tasks can not currently do proper error handling (TODO).

        This means that for timeout canceled tasks:
         - no html file is generated
         - evaluated cells may not be present in output notebook
         - the temp ipynb file is not deleted
        """
        # html file version of notebook might be missing
        assert set(["runlog", "other_files"]) <= data.keys()

        # runlog content should describe the failure
        assert data["runlog"]["parameters.run.run_directory"] == str(out_path / "stuck")
        assert data["runlog"]["task_id"] == "id=stuck"
        assert data["runlog"]["out.status"] == "FAILURE"
        assert (
            data["runlog"]["out.error"]
            == "Timeout error: execution did not finish within timeout limit"
        )

        # temp ipynb notebook file may or may not have been deleted
        assert len(data["other_files"].keys()) <= 1

    validate_stuck_notebook_run_directory(get_data(out_path / "stuck"))

    # ----
    def validate_exception_notebook_run_directory(data):
        """
        An exception thrown inside the notebook Python process should be captured and
        properly handled.

        So, these notebooks should fail but without any of the error handling
        difficulties of notebooks that need to be canceled.
        """
        assert set(["ipynb", "html", "runlog", "other_files"]) == data.keys()

        for expected_string in [str(1 + 12 + 123)]:
            assert expected_string in data["ipynb"]

        # runlog content
        assert data["runlog"]["parameters.run.run_directory"] == str(
            out_path / "exception"
        )
        assert data["runlog"]["task_id"] == "id=exception"
        assert data["runlog"]["out.status"] == "FAILURE"
        assert "Exception: Thrown from notebook!" in data["runlog"]["out.error"]

        assert data["other_files"] == dict()

    validate_exception_notebook_run_directory(get_data(out_path / "exception"))
