from typing import Mapping
from pathlib import Path

#
import opentelemetry as otel
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

#
from pynb_dag_runner.core.dag_runner import task_from_python_function
from pynb_dag_runner.opentelemetry_helpers import AttributesDict
from pynb_dag_runner.tasks.task_opentelemetry_logging import _log_named_value

#
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook


def prefix_keys(prefix: str, a_dict: AttributesDict) -> AttributesDict:
    return {f"{prefix}.{k}": v for k, v in a_dict.items()}


def _get_traceparent() -> str:
    """
    Get implicit OpenTelemetry span context for context propagation (to notebooks)
    """
    carrier: Mapping[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier=carrier)

    # check that context `carrier` dict is of type {"traceparent": <some string>}
    assert isinstance(carrier, dict)
    assert carrier.keys() == {"traceparent"}
    assert isinstance(carrier["traceparent"], str)

    return carrier["traceparent"]


def make_jupytext_task_ot(
    notebook: JupytextNotebook,
    tmp_dir: Path,
    timeout_s: float = None,
    max_nr_retries: int = 1,
    num_cpus: int = 1,
    parameters: AttributesDict = {},
):
    # Determine task run-attributes (except baggage which can only be determined at
    # run time).
    run_attributes: AttributesDict = {
        **parameters,
        "task.notebook": str(notebook.filepath),
    }

    def run_notebook(arg):
        tmp_filepath: Path = (tmp_dir / notebook.filepath.name).with_suffix(".ipynb")
        evaluated_notebook = JupyterIpynbNotebook(tmp_filepath)

        baggage = otel.baggage.get_all()

        try:
            notebook.evaluate(
                output=evaluated_notebook,
                parameters={
                    "P": {
                        **run_attributes,
                        **baggage,
                        "_opentelemetry_traceparent": _get_traceparent(),
                    }
                },
            )

        except BaseException as e:
            raise e

        finally:
            # this is not run if notebook is killed by timeout
            _log_named_value(
                name="notebook.ipynb",
                content=evaluated_notebook.filepath.read_text(),
                content_type="utf-8",
                is_file=True,
            )

    return task_from_python_function(
        f=run_notebook,
        num_cpus=num_cpus,
        max_nr_retries=max_nr_retries,
        timeout_s=timeout_s,
        attributes=run_attributes,
        task_type="jupytext",
    )
