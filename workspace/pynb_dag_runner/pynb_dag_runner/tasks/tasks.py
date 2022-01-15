import uuid
from pathlib import Path
from typing import Awaitable, Callable, Dict, Mapping, Any, Optional

#
import ray
import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore

#
from pynb_dag_runner.core.dag_runner import task_from_python_function
from pynb_dag_runner.core.dag_runner import Task
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.wrappers.compute_steps import (
    T,
    AddTiming,
    AddPythonFunctionCall,
    AddParameters,
    AddDynamicParameter,
    AddRetries,
    AddCreateRunlogOutputPath,
    AddPersistRunlog,
)

#
from pynb_dag_runner.ray_helpers import (
    Future,
    _try_eval_f_async_wrapper,
    retry_wrapper_ot_deprecated,
)
from pynb_dag_runner.helpers import compose
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook

RunParameters = Mapping[str, Any]


def prefix_keys(prefix: str, a_dict: RunParameters) -> RunParameters:
    return {f"{prefix}.{k}": v for k, v in a_dict.items()}


class PythonFunctionTask_OT(Task[bool]):
    # -- deprecated, to be deleted --
    def __init__(
        self,
        f: Callable[[RunParameters], Any],
        task_id: str,
        timeout_s: float = None,
        n_max_retries: int = 1,
        common_task_runparameters: RunParameters = {},
    ):
        """
        (to replace old PythonFunctionTask below)

        Task[None] to execute Python function with OpenTelemetry logging

        task_id:s should be 1-1 with the nodes in the dependency DAG, but one
        task_id may correspond to many run_id:s if there are retries.
        """
        assert n_max_retries >= 1
        self.task_id = task_id

        f_remote: Callable[
            [RunParameters], Awaitable[bool]
        ] = _try_eval_f_async_wrapper(
            f=lambda runparameters: f(runparameters),
            timeout_s=timeout_s,
            success_handler=lambda _: True,
            error_handler=lambda _: False,
        )

        # Ray supports async methods in Actors, but that here generates error message:
        #
        # AttributeError: 'NoneType' object has no attribute '_ray_is_cross_language'
        #
        async def task_run(invocation_runparameters: RunParameters) -> Any:
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("task-run") as span:

                # determine runparameters for this task invocation
                runparameters = {
                    **invocation_runparameters,
                    **common_task_runparameters,
                    "task_id": task_id,
                }

                # log runparameters to span
                for k, v in runparameters.items():
                    span.set_attribute(k, v)

                is_success: bool = await f_remote(ray.put(runparameters))  # type: ignore
                if is_success:
                    span.set_status(Status(StatusCode.OK))
                else:
                    span.set_status(Status(StatusCode.ERROR, "Run failed"))

            return is_success

        async def retry_wrapper(runparameters: RunParameters) -> bool:
            retry_arguments = [
                {
                    **runparameters,
                    "retry.max_retries": n_max_retries,
                    "retry.nr": retry_nr,
                }
                for retry_nr in range(n_max_retries)
            ]

            return await retry_wrapper_ot_deprecated(task_run, retry_arguments)

        async def invoke_task(runparameters: RunParameters) -> bool:
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("invoke-task") as span:
                span.set_attribute("task_type", "python")
                span.set_attribute("task_id", task_id)
                is_success: bool = await retry_wrapper(runparameters=runparameters)
                if is_success:
                    span.set_status(Status(StatusCode.OK))
                else:
                    span.set_status(Status(StatusCode.ERROR, "Task failed"))

            return is_success

        super().__init__(f_remote=Future.lift_async(invoke_task, num_cpus=1))  # type: ignore


class PythonFunctionTask(Task[Runlog]):
    # -- deprecated, to be deleted --
    def __init__(
        self,
        f,
        task_id: str,
        get_run_path: Optional[Callable[[Runlog], Path]] = None,
        timeout_s: float = None,
        n_max_retries: int = 1,
        parameters: Dict[str, Any] = {},
    ):
        # task_id:s should be 1-1 with the nodes in the dependency DAG
        # one task_id may correspond to many run_id:s
        self.task_id = task_id

        steps: T[T[Future[Runlog]]] = compose(
            AddPersistRunlog(),
            AddTiming(),
            AddPythonFunctionCall(f, timeout_s=timeout_s),
            AddCreateRunlogOutputPath(get_run_path=get_run_path),
            AddDynamicParameter("parameters.run.id", lambda _: str(uuid.uuid4())),
            AddDynamicParameter("task_id", lambda _: self.task_id),
            AddParameters(**parameters),
        )
        super().__init__(
            f_remote=lambda _: (
                compose(AddRetries(n_max_retries=n_max_retries), steps)(
                    lambda _runlog_future: _runlog_future
                )
            )(Future.value(Runlog()))
        )


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
    task_parameters: RunParameters = {},
    tags: Any = {},
):
    def run_notebook(arg):
        tmp_filepath: Path = (tmp_dir / notebook.filepath.name).with_suffix(".ipynb")
        evaluated_notebook = JupyterIpynbNotebook(tmp_filepath)

        baggage = otel.baggage.get_all()

        try:
            notebook.evaluate(
                output=evaluated_notebook,
                parameters={
                    "P": {**baggage, **prefix_keys("task_parameter", task_parameters)}
                },
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
        tags={**tags, "notebook": str(notebook.filepath)},
        task_type="jupytext",
    )
