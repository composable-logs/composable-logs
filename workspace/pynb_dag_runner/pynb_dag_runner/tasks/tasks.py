import uuid
from pathlib import Path
from typing import Callable, Dict, Mapping, Any, Optional

#
import ray
import opentelemetry as otel

#
from pynb_dag_runner.core.dag_runner import Task, TaskDependencies
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
from pynb_dag_runner.ray_helpers import Future, try_eval_f_async_wrapper
from pynb_dag_runner.helpers import compose
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook

RunParameters = Mapping[str, Any]


class PythonFunctionTask_OT(Task[bool]):
    def __init__(
        self,
        f: Callable[[RunParameters], Any],
        task_id: str,
        timeout_s: float = None,
        # n_max_retries: int = 1, TODO
        common_task_runparameters: RunParameters = {},
    ):
        """
        (to replace old PythonFunctionTask below)

        Task[None] to execute Python function with OpenTelemetry logging

        task_id:s should be 1-1 with the nodes in the dependency DAG, but one
        task_id may correspond to many run_id:s if there are retries.
        """
        self.task_id = task_id

        f_remote: Callable[
            [Future[RunParameters]], Future[bool]
        ] = try_eval_f_async_wrapper(
            f=lambda runparameters: f(runparameters),
            timeout_s=timeout_s,
            success_handler=lambda _: True,
            error_handler=lambda _: False,
        )

        # Ray supports async methods in Actors, but that here generates error message:
        #
        # AttributeError: 'NoneType' object has no attribute '_ray_is_cross_language'
        #
        async def wrapped(invocation_runparameters: RunParameters) -> Any:
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("python-task") as span:

                # determine runparameters for this task invocation
                runparameters = {
                    **invocation_runparameters,
                    **common_task_runparameters,
                    "task_id": task_id,
                    "run_id": str(uuid.uuid4()),
                }

                # log runparameters to span
                for k, v in runparameters.items():
                    span.set_attribute(k, v)

                result = await f_remote(ray.put(runparameters))  # type: ignore

            return result

        super().__init__(f_remote=Future.lift_async(wrapped))


class PythonFunctionTask(Task[Runlog]):
    # -- to be deleted --
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


class JupytextNotebookTask(PythonFunctionTask):
    def __init__(
        self,
        notebook: JupytextNotebook,
        task_id: str,
        get_run_path: Callable[[Runlog], Path],
        timeout_s: float = None,
        n_max_retries: int = 1,
        parameters: Dict[str, Any] = {},
    ):
        def f(runlog: Runlog):
            evaluated_notebook = JupyterIpynbNotebook(
                (get_run_path(runlog) / notebook.filepath.name).with_suffix(".ipynb")
            )

            try:
                notebook.evaluate(
                    output=evaluated_notebook,
                    parameters={"P": runlog.as_dict(prefix_filter="parameters.")},
                )
            finally:
                evaluated_notebook.to_html()

        super().__init__(
            f=f,
            task_id=task_id,
            get_run_path=get_run_path,
            timeout_s=timeout_s,
            n_max_retries=n_max_retries,
            parameters=parameters,
        )


def get_task_dependencies(dependencies: TaskDependencies):
    """
    This function assumes that each Task (or Edge) instance referenced in dependencies
    contains an task_id-attribute. See eg. PythonFunctionTask.

    This function converts this input into a native Python list with
    the task dependencies. This can, for example, be serialized to a JSON object
    """
    return [
        {
            "from": edge.from_node.task_id,
            "to": edge.to_node.task_id,
        }
        for edge in dependencies
    ]
