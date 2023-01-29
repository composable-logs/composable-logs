from typing import Optional, Mapping
from pathlib import Path

# -
import opentelemetry as otel
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

# -
from pynb_dag_runner.opentelemetry_helpers import AttributesDict
from pynb_dag_runner.tasks.task_opentelemetry_logging import _log_named_value

# -
from pynb_dag_runner.notebooks_helpers import JupytextNotebookContent
from ..wrappers import task


def _get_traceparent() -> str:
    """
    Get implicit OpenTelemetry span context for context propagation (eg. to notebooks)
    """
    carrier: Mapping[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier=carrier)

    # check that context `carrier` dict is of type {"traceparent": <some string>}
    assert isinstance(carrier, dict)
    assert carrier.keys() == {"traceparent"}
    assert isinstance(carrier["traceparent"], str)

    return carrier["traceparent"]


def make_jupytext_task(
    notebook: JupytextNotebookContent,
    cwd: Optional[Path] = None,
    timeout_s: Optional[float] = 60.0,
    num_cpus: int = 1,
    parameters: AttributesDict = {},
):
    """
    Make task from a Jupytext notebook that can be run in a Composable-Logs DAG.

    Notes:
     - Currently, timeout canceling is done on Ray level, but error handling and
       is done only within the Python process (using try .. catch).

       Therefore, timeout-canceled tasks can not currently do proper error handling.
       Eg., there would be no notebook artifact logged from a timeout-canceled task.

       The solution would be to enable OpenTelemetry logging for Papermill on a
       per-cell level.
    """
    task_parameters: AttributesDict = {
        **parameters,
        "task.task_type": "jupytext",
        # TODO: Remove "task.notebook" and make tasks depend on task.task_id instead.
        # This allows better suport for non-notebook tasks.
        "task.notebook": str(notebook.filepath),
    }

    @task(
        task_id=str(notebook.filepath),
        task_parameters=task_parameters,
        timeout_s=timeout_s,
        num_cpus=num_cpus,
    )
    def run_notebook_task(*dummy_args, **kwargs):
        # we accept positional args, but only so we can chain notebooks.

        baggage = otel.baggage.get_all()

        for k in kwargs.keys():
            if not k.startswith("task."):
                raise ValueError(
                    "Notebook task received parameter {k}. "
                    "All task parameter names should start with 'task.'"
                )

        # The below return type is not a Try. Even in case of error, we would obtain
        # a partially evaluated notebook.
        err, evaluated_notebook = notebook.to_ipynb().evaluate(
            cwd=cwd,
            parameters={
                "P": {
                    **kwargs,
                    **task_parameters,
                    **baggage,
                    "_opentelemetry_traceparent": _get_traceparent(),
                }
            },
        )

        # this is not run if notebook is killed by timeout
        _log_named_value(
            name="notebook.ipynb",
            content=evaluated_notebook.content,
            content_type="utf-8",
            is_file=True,
        )

        if err is not None:
            raise err

    return run_notebook_task
