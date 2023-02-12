from typing import Optional
from pathlib import Path

# -
import opentelemetry as otel

# -
from composable_logs.opentelemetry_helpers import AttributesDict
from composable_logs.tasks.task_opentelemetry_logging import _log_named_value

# -
from composable_logs.notebooks_helpers import JupytextNotebookContent
from ..wrappers import _task


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

    @_task(
        # task_id:
        # Set to notebook filename without an extension as string. Eg
        # /path/to/ingestion-notebook.py -> ingestion-notebook
        # The assumption is that we do not have notebooks with the same filenames in
        # different directories
        task_id=notebook.filepath.stem,
        task_type="jupytext",
        task_parameters=parameters,
        timeout_s=timeout_s,
        num_cpus=num_cpus,
    )
    def run_notebook_task(*dummy_args, **kwargs):
        # we accept positional args, but only so we can chain notebooks.

        for k in kwargs.keys():
            if not k.startswith("task."):
                raise ValueError(
                    "Notebook task received parameter {k}. "
                    "All task parameter names should start with 'task.'"
                )

        P = {"_parameters_actor_name": otel.baggage.get_all()["_parameters_actor_name"]}

        # The below return type is not a Try. Even in case of error, we would obtain
        # a partially evaluated notebook.
        err, evaluated_notebook = notebook.to_ipynb().evaluate(
            cwd=cwd,
            parameters={"P": {**parameters, **P}},
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
