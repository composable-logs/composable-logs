import asyncio, random
from typing import Any, TypeVar, Callable, List, Optional, Awaitable

#
import ray
import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore

#
from pynb_dag_runner.helpers import Try
from pynb_dag_runner.opentelemetry_helpers import otel_add_baggage

A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


class RayMypy:
    """
    Dummy class to avoid generate missing .remote attribute error on constructor calls
    like:

      SubClass.remote(<class SubClass constructor arguments>)

    Ray will warn about this existing before overwriting this at runtime, so this code
    is never run, but makes type checking better.
    """

    @classmethod
    def remote(cls, *args, **kwargs):
        if random.random() < 0.5:
            raise Exception("This should never run")
        return cls(*args, **kwargs)


class Future(Awaitable[A]):
    """
    Helper class that can be used to add type hints for Ray Futures (or object ref:s).

    We can not type check all interfaces to Ray, but type hints can be used to document
    the code and at least catch some type errors.

    See: https://github.com/ray-project/ray/blob/master/python/ray/types.py
    """

    @staticmethod
    def value(a: A) -> Awaitable[A]:
        return ray.put(a)

    @staticmethod
    def map(future: Awaitable[A], f: Callable[[A], B]) -> Awaitable[B]:
        """
        Return a new Future with the value of future mapped through f.

        TODO: argument order should be consistent with Python's map.
        """

        @ray.remote(num_cpus=0)
        def do_map(future_value: A) -> B:
            return f(future_value)

        return do_map.remote(future)

    @staticmethod
    def lift_async(
        f: Callable[[B], Awaitable[C]], num_cpus: int = 0
    ) -> Callable[[B], Awaitable[C]]:
        """
        Lift an async Python function f as below

        ```
        async def f(b: B) -> C:
            ...
        ```

        into a Ray remote function operating on Ray object ref:s.

        See: https://docs.ray.io/en/latest/async_api.html
        """

        @ray.remote(num_cpus=num_cpus)
        def wrapped_f(arg: B) -> C:
            return asyncio.get_event_loop().run_until_complete(f(arg))

        return wrapped_f.remote

    @staticmethod
    def lift(f: Callable[[B], C]) -> Callable[[Awaitable[B]], Awaitable[C]]:
        return lambda future: Future.map(future, f)


def _try_eval_f_async_wrapper(
    f: Callable[[A], B],
    timeout_s: Optional[float],
    success_handler: Callable[[B], C],
    error_handler: Callable[[Exception], C],
    num_cpus: int = 0,
) -> Callable[[A], Awaitable[C]]:
    """
    Lift a function f: A -> B and result/error handlers into a function operating
    on futures Future[A] -> Future[C].

    The lifted function logs to OpenTelemetry
    """

    @ray.remote(num_cpus=num_cpus)
    class ExecActor:
        def call(self, *args):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore

            # Execute function in separate OpenTelemetry span.
            with tracer.start_as_current_span("call-python-function") as span:

                span.set_attribute("task.num_cpus", num_cpus)
                otel_add_baggage("task.num_cpus", num_cpus)

                try:
                    result = success_handler(f(*args))
                    span.set_status(Status(StatusCode.OK))

                except BaseException as e:
                    result = error_handler(e)
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, "Failure"))

            return result

    async def timeout_guard(a: A) -> C:
        """
        See also Ray issues:
         - "Support timeout option in Ray tasks",
           https://github.com/ray-project/ray/issues/17451
         - "Set time-out on individual ray task"
           https://github.com/ray-project/ray/issues/15672
        """
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("timeout-guard") as span:
            span.set_attribute("task.timeout_s", timeout_s)
            otel_add_baggage("task.timeout_s", timeout_s)

            work_actor = ExecActor.remote()  # type: ignore
            future = work_actor.call.remote(a)

            refs_done, refs_not_done = ray.wait(
                [future], num_returns=1, timeout=timeout_s
            )
            assert len(refs_done) + len(refs_not_done) == 1
            if len(refs_done) == 1:
                assert refs_done == [future]
                span.set_status(Status(StatusCode.OK))
                return await future
            else:
                assert refs_not_done == [future]
                span.set_status(Status(StatusCode.ERROR, "Timeout"))

                # https://docs.ray.io/en/latest/actors.html#terminating-actors
                ray.kill(work_actor)

                return error_handler(
                    Exception(
                        "Timeout error: execution did not finish within timeout limit"
                    )
                )

    return timeout_guard


def try_f_with_timeout_guard(
    f: Callable[[A], B], timeout_s: Optional[float], num_cpus: int
) -> Callable[[A], Awaitable[Try[B]]]:
    return _try_eval_f_async_wrapper(
        f=f,
        timeout_s=timeout_s,
        num_cpus=num_cpus,
        success_handler=lambda f_result: Try(value=f_result, error=None),
        error_handler=lambda f_exception: Try(value=None, error=f_exception),
    )
