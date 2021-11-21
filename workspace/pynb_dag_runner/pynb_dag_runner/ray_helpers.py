import asyncio, inspect
from typing import Any, Coroutine, TypeVar, Generic, Callable, List, Optional, Awaitable

#
import ray
import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore


A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


class Future(Generic[A]):
    """
    Helper class that can be used to add type hints for Ray Futures (or object ref:s).

    We can not type check all interfaces to Ray, but type hints can be used to document
    the code and at least catch some type errors.
    """

    @staticmethod
    def value(a: A) -> "Future[A]":
        return ray.put(a)

    @staticmethod
    def map(future: "Future[A]", f: Callable[[A], B]) -> "Future[B]":
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
    ) -> "Callable[[Future[B]], Future[C]]":
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
        def wrapped_f(*args: B) -> C:
            return asyncio.get_event_loop().run_until_complete(f(*args))

        return wrapped_f.remote

    @staticmethod
    def lift(f: Callable[[B], C]) -> "Callable[[Future[B]], Future[C]]":
        return lambda future: Future.map(future, f)


def try_eval_f_async_wrapper(
    f: Callable[[A], B],
    timeout_s: Optional[float],
    success_handler: Callable[[B], C],
    error_handler: Callable[[Exception], C],
) -> Callable[[Future[A]], Future[C]]:
    """
    Lift a function f: A -> B and result/error handlers into a function operating
    on futures Future[A] -> Future[C].

    The lifted function logs to OpenTelemetry
    """

    @ray.remote(num_cpus=0)
    class ExecActor:
        def call(self, *args):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore

            # Execute function in separate OpenTelemetry span.
            with tracer.start_as_current_span("call-python-function") as span:
                try:
                    result = success_handler(f(*args))
                    span.set_status(Status(StatusCode.OK))

                except Exception as e:
                    result = error_handler(e)
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, "Failure"))

            return result

    def timeout_guard(a: A) -> C:
        """
        See also Ray issues:
         - "Support timeout option in Ray tasks",
           https://github.com/ray-project/ray/issues/17451
         - "Set time-out on individual ray task"
           https://github.com/ray-project/ray/issues/15672
        """
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("timeout-guard") as span:
            span.set_attribute("timeout_s", timeout_s)

            work_actor = ExecActor.remote()  # type: ignore
            future = work_actor.call.remote(a)

            refs_done, refs_not_done = ray.wait(
                [future], num_returns=1, timeout=timeout_s
            )
            assert len(refs_done) + len(refs_not_done) == 1
            if len(refs_done) == 1:
                assert refs_done == [future]
                span.set_status(Status(StatusCode.OK))
                return ray.get(future)
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

    return Future.lift(timeout_guard)


RetryCount = int


def retry_wrapper(
    f_task_remote: Callable[[RetryCount], Future[A]],
    max_retries: RetryCount,
    is_success: Callable[[A], bool],
) -> Future[List[A]]:
    # -- deprecated --
    # Note: this is currently the only place where cpu-resources are allocated for Ray
    @ray.remote(num_cpus=1)
    class RetryActor:
        async def make_retry_calls(self):
            results: List[A] = []
            for attempt_nr in range(max_retries):
                results.append(await f_task_remote(attempt_nr))

                if is_success(results[-1]):
                    break

            return results

    retry_actor = RetryActor.remote()  # type: ignore
    return retry_actor.make_retry_calls.remote()


def retry_wrapper_ot(
    f_task_remote: Callable[[A], Coroutine[A, None, bool]],
    retry_arguments: List[A],
) -> Awaitable[bool]:
    @ray.remote(num_cpus=0)
    class RetryActor:
        async def make_retry_calls(self):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("retry-wrapper") as span:
                for arg in retry_arguments:
                    if await f_task_remote(arg):
                        span.set_status(Status(StatusCode.OK))
                        return True

                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        f"Task retried {len(retry_arguments)} times; all failed!",
                    )
                )
                return False

    retry_actor = RetryActor.remote()  # type: ignore
    return retry_actor.make_retry_calls.remote()
