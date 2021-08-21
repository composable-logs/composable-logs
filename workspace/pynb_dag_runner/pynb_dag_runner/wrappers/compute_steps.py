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

        return compose(Future.lift(add_parameters), t)


class AddDynamicParameter(TaskFunctionWrapper):
    """
    Add a dynamic parameter to runlog that may depend on existing values in the runlog.

    ----
    Note: To motivate the below compose-order, consider
      Tx: T[Future[Runlog]] = AddDynamicParameter("x", add_x) [add x=1 to runlog]
      Ty: T[Future[Runlog]] = AddDynamicParameter("y", add_y) [add y=x + 1 to runlog]

    Definitions of Tx, Ty and compose then give:

    (compose(Ty, Tx)(id))(r0)
      = Ty(Tx(id))(r0)
      = compose(Future.lift(add_y), Tx(id))(r0)
      = Future.lift(add_y)(Tx(id)(r0))
      = Future.lift(add_y)(compose(Future.lift(add_x), id)(r0))
      = Future.lift(add_y)(Future.lift(add_x)(r0))

    where r0 = Future.value(Runlog()) and id is the identity map in Future[Runlog].

    Thus with this setup, y (which depends on x) is added after x.
    """

    def __init__(self, parameter_name: str, f: Callable[[Runlog], Any]):
        self.f = f
        self.parameter_name = parameter_name

    def __call__(self, t: T[Future[Runlog]]) -> T[Future[Runlog]]:
        def add_parameter(runlog: Runlog) -> Runlog:
            return runlog.add(**{self.parameter_name: self.f(runlog)})

        return compose(Future.lift(add_parameter), t)
