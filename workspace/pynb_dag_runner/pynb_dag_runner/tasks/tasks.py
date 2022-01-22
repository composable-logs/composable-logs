from pathlib import Path
from typing import Dict, Mapping, Any

#
import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore

#
from pynb_dag_runner.core.dag_runner import task_from_python_function, AttributesDict

#
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook


def prefix_keys(prefix: str, a_dict: AttributesDict) -> AttributesDict:
    return {f"{prefix}.{k}": v for k, v in a_dict.items()}


def log_artefact(name, content):
    tracer = otel.trace.get_tracer(__name__)  # type: ignore
    with tracer.start_as_current_span("artefact") as span:
        span.set_attribute("name", name)
        span.set_attribute("content", content)
        span.set_status(Status(StatusCode.OK))


def make_jupytext_task_ot(
    notebook: JupytextNotebook,
    tmp_dir: Path,
    timeout_s: float = None,
    max_nr_retries: int = 1,
    num_cpus: int = 1,
    parameters: AttributesDict = {},
):
    # run-attributes sans-baggage which can only be determined at run time

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
                parameters={"P": {**run_attributes, **baggage}},
            )

        except BaseException as e:
            raise e

        finally:
            # this is not run if notebook is killed by timeout
            log_artefact("notebook.ipynb", evaluated_notebook.filepath.read_text())

    return task_from_python_function(
        f=run_notebook,
        num_cpus=num_cpus,
        max_nr_retries=max_nr_retries,
        timeout_s=timeout_s,
        attributes=run_attributes,
        task_type="jupytext",
    )
