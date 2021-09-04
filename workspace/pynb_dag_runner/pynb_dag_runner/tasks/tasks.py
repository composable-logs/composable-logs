import uuid
from pathlib import Path
from typing import Callable, Dict, Any, Optional

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
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.helpers import compose
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook


class PythonFunctionTask(Task[Runlog]):
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
            f_remote=lambda: (
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
