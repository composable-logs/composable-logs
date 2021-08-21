import time, os
from pathlib import Path
from typing import TypeVar, Generic, Callable, Dict, Any

#
import ray

#
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.helpers import compose

A = TypeVar("A")
T = Callable[[A], A]


class TaskFunctionWrapper:
    pass


class AddParameters(TaskFunctionWrapper):
    """
    Wrapper to add static parameters (provided as a dict) to the runlog
    """

    def __init__(self, **run_parameters: Dict[str, Any]):
        self.run_parameters = run_parameters

    def __call__(self, t: T[Future[Runlog]]) -> T[Future[Runlog]]:
        def add_parameters(runlog: Runlog) -> Runlog:
            return runlog.add(**self.run_parameters)

        return compose(t, Future.lift(add_parameters))
