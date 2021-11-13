import datetime
from pathlib import Path
from typing import Any, Dict

#
import pytest

#
from pynb_dag_runner.tasks.tasks import make_jupytext_task
from pynb_dag_runner.core.dag_runner import run_tasks, TaskDependencies
from pynb_dag_runner.helpers import one
from pynb_dag_runner.notebooks_helpers import JupytextNotebook
from pynb_dag_runner.opentelemetry_helpers import Spans, Span, SpanRecorder

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


# ----

"""
Data types for representing tasks recovered from OpenTelemetry traces emitted by
pynb-dag-runner and Ray.
"""

from typing import List, Optional
import dataclasses
from dataclasses import dataclass
from pynb_dag_runner.tasks.tasks import RunParameters


@dataclass
class _LoggedSpan:
    span_id: str


@dataclass
class _LoggedOutcome:
    is_success: bool
    error: Optional[str]


class _To_Dict:
    def as_dict(self):
        return dataclasses.asdict(self)


@dataclass
class LoggedTaskRun(_LoggedSpan, _LoggedOutcome, _To_Dict):
    run_parameters: RunParameters


@dataclass
class LoggedTask(_LoggedSpan, _LoggedOutcome, _To_Dict):
    task_id: str
    task_parameters: RunParameters
    runs: List[LoggedTaskRun]


@dataclass
class LoggedJupytextTask(LoggedTask):
    task_type: str = "jupytext"


def make_jupytext_logged_task(
    jupytext_span: Span, all_spans: Spans
) -> LoggedJupytextTask:
    is_success = jupytext_span["status"]["status_code"] == "OK"

    return LoggedJupytextTask(
        span_id=jupytext_span["context"]["span_id"],
        is_success=is_success,
        error=None if is_success else jupytext_span["status"]["description"],
        task_id=jupytext_span["attributes"]["task_id"],
        task_parameters=jupytext_span["attributes"],
        runs=[],
    )


def get_tasks(spans: Spans) -> List[LoggedTask]:
    """
    Convert a list of otel spans into a list of LoggedTask for easier processing.

    Notes:
     - this only outputs jupytext tasks.
     - jupytext tasks are assumed to not be nested.

    """
    jupytext_spans = spans.filter(["name"], "invoke-task").filter(
        ["attributes", "task_type"], "jupytext"
    )

    return [make_jupytext_logged_task(jt_span, spans) for jt_span in jupytext_spans]


# ----


def make_test_nb_task(nb_name: str, n_max_retries: int, task_parameters={}):
    nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"
    return make_jupytext_task(
        notebook=JupytextNotebook(nb_path / nb_name),
        task_id=f"{nb_name}-task",
        tmp_dir=nb_path,
        timeout_s=5,
        n_max_retries=n_max_retries,
        task_parameters=task_parameters,
    )


def test_jupytext_run_ok_notebook():
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_ok.py",
                n_max_retries=5,
                task_parameters={"variable_a": "task-value"},
            )
            run_tasks([jupytext_task], TaskDependencies())

        return rec.spans

    def validate_spans(spans: Spans):
        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        assert jupytext_span["status"] == {"status_code": "OK"}

        py_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "python"
            )
        )
        assert py_span["status"] == {"status_code": "OK"}

        artefact_span = one(spans.filter(["name"], "artefact"))
        for content in [str(1 + 12 + 123), "variable_a=task-value"]:
            assert content in artefact_span["attributes"]["content"]

        spans.contains_path(jupytext_span, py_span, artefact_span)

    def validate_recover_tasks_from_spans(spans: Spans):
        extracted_task = one(get_tasks(spans))

        assert isinstance(extracted_task, LoggedJupytextTask)
        assert extracted_task.is_success == True
        assert extracted_task.error is None
        # assert extracted_task.task_parameters.keys() == {}
        # assert len(extracted_task.output) > 0
        # assert len(extracted_task.runs) > 0

    spans = get_test_spans()
    validate_spans(spans)
    validate_recover_tasks_from_spans(spans)


@pytest.mark.parametrize("N_retries", [2, 10])
def test_jupytext_exception_throwing_notebook(N_retries):
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_exception.py",
                n_max_retries=N_retries,
                task_parameters={},
            )
            run_tasks([jupytext_task], TaskDependencies())

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

        for idx in failed_indices() + ok_indices():
            # for both successful and failed runs, a partially evaluated notebook
            # should have been logged as an artefact.
            artefact_span = one(
                spans.restrict_by_top(run_spans[idx]).filter(["name"], "artefact")
            )
            assert artefact_span["attributes"]["name"] == "notebook.ipynb"
            assert str(1 + 12 + 123) in artefact_span["attributes"]["content"]

    validate_spans(get_test_spans())


def test_jupytext_stuck_notebook():
    """
    Currently, timeout canceling is done on Ray level, but error handling and
    recovery is done only within the Python process (using try .. catch).
    Therefore, timeout canceled tasks can not currently do proper error handling.
    """

    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_stuck.py",
                n_max_retries=1,
                task_parameters={},
            )
            run_tasks([jupytext_task], TaskDependencies())

        return rec.spans

    def validate_spans(spans: Spans):
        py_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "python"
            )
        )
        assert py_span["status"] == {
            "description": "Task failed",
            "status_code": "ERROR",
        }

        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        assert jupytext_span["status"] == {
            "description": "Jupytext notebook task failed",
            "status_code": "ERROR",
        }

        timeout_guard_span = one(spans.filter(["name"], "timeout-guard"))
        assert timeout_guard_span["status"] == {
            "status_code": "ERROR",
            "description": "Timeout",
        }

        spans.contains_path(jupytext_span, timeout_guard_span, py_span)

        assert len(spans.exceptions_in(jupytext_span)) == 0

        # notebook evaluation never finishes, and no ipynb is logged
        assert len(spans.filter(["name"], "artefact")) == 0

    validate_spans(get_test_spans())
