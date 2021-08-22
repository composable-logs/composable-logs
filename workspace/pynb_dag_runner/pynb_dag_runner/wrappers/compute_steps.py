import time, os
from pathlib import Path
from typing import TypeVar, Generic, Callable, Dict, Any, Optional

#
import ray

#
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.ray_helpers import Future, try_eval_f_async_wrapper
from pynb_dag_runner.helpers import compose

A = TypeVar("A")

# Define type for functions (or transforms) A -> A. Eg. the identity function: A -> A
# would have type T[A].
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


class AddPythonFunctionCall(TaskFunctionWrapper):
    """
    Wrapper to execute a Python function (of signature f: Runlog -> Any).

    The argument timeout_s specifies maximum seconds that the function may run before
    the execution is canceled (with an exception). When timeout_s is None there is no
    timeout limit.

    The return value for the function (or any exception thrown, like for a timeout) is
    stored to the runlog.
    """

    def __init__(self, f: Callable[[Runlog], Any], timeout_s: Optional[float] = None):
        self.f = f
        self.timeout_s = timeout_s

    def __call__(self, t: T[Future[Runlog]]) -> T[Future[Runlog]]:
        def eval_f(runlog_future: Future[Runlog]) -> Future[Runlog]:
            f_result_future = try_eval_f_async_wrapper(
                f=self.f,
                timeout_s=self.timeout_s,
                success_handler=lambda f_result: {
                    "out.status": "SUCCESS",
                    "out.result": f_result,
                    "out.error": None,
                },
                error_handler=lambda exception: {
                    "out.status": "FAILURE",
                    "out.result": None,
                    "out.error": str(exception),
                },
            )(runlog_future)

            @ray.remote(num_cpus=0)
            def add_keys(runlog: Runlog, new_values: Dict) -> Runlog:
                return runlog.add(**new_values)

            return add_keys.remote(runlog_future, f_result_future)

        add_timeout_s: T[T[Future[Runlog]]] = AddDynamicParameter(
            "parameters.task.timeout_s", lambda _: self.timeout_s
        )

        return compose(eval_f, add_timeout_s(t))


class AddTiming(TaskFunctionWrapper):
    """
    Wrapper to add timing data to runlog
    """

    def __call__(self, t: T[Future[Runlog]]) -> T[Future[Runlog]]:
        def add_timing(runlog_future: Future[Runlog]) -> Future[Runlog]:
            @ray.remote(num_cpus=0)
            def run_pre_t(_: Runlog) -> int:
                return time.time_ns()

            @ray.remote(num_cpus=0)
            def run_post_t(pre_t_ts: int, post_t_runlog: Runlog) -> Runlog:
                post_t_ts: int = time.time_ns()

                # TODO: can mypy type ignore:s be removed here?
                return post_t_runlog.add(  # type: ignore
                    **{  # type: ignore
                        "out.timing.start_ts": pre_t_ts,  # type: ignore
                        "out.timing.end_ts": post_t_ts,  # type: ignore
                        "out.timing.duration_ms": (post_t_ts - pre_t_ts) // 1000000,  # type: ignore
                    }  # type: ignore
                )  # type: ignore

            # Note: by passing runlog_future as an argument to both of the above
            # functions, neither is started before runlog_future (=previous steps
            # in computation) are finished.
            return run_post_t.remote(run_pre_t.remote(runlog_future), t(runlog_future))

        return add_timing
