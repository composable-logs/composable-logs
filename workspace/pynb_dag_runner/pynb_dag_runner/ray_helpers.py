import asyncio, random
from typing import Any, Coroutine, TypeVar, Generic, Callable, List, Optional, Awaitable

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


@ray.remote(num_cpus=0)
class FutureActor:
    """
    Ray actor containing future value that can be set and awaited independently.

    Based on example code from Ray docs, see
    https://docs.ray.io/en/latest/advanced.html

    Note:
    Adding (Generic[A]) to class definition gives error
       _pickle.PicklingError: Can't pickle <functools._lru_cache_wrapper object at ..>:
       it's not the same object as typing.Generic.__class_getitem__
    """

    def __init__(self):
        self._ready_event = asyncio.Event()
        self._value: Optional[A] = None
        self._value_is_set = False

    def set_value(self, new_value) -> None:
        if self._value_is_set:
            raise Exception("Value already set")
        self._value = new_value
        self._value_is_set = True
        self._ready_event.set()

    def value_is_set(self) -> bool:
        return self._value_is_set

    async def wait(self) -> Any:
        if self._value_is_set:
            return self._value
        await self._ready_event.wait()
        return self._value


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

                span.set_attribute("num_cpus", num_cpus)
                otel_add_baggage("num_cpus", num_cpus)

                try:
                    result = success_handler(f(*args))
                    span.set_status(Status(StatusCode.OK))

                except Exception as e:
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
            span.set_attribute("timeout_s", timeout_s)
            otel_add_baggage("timeout_s", timeout_s)

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


def retry_wrapper_ot_deprecated(
    f_task_remote: Callable[[A], Awaitable[bool]],
    retry_arguments: List[A],
) -> Awaitable[bool]:
    # -- deprecated --
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


def retry_wrapper_ot(
    f: Callable[[A], Awaitable[Try[B]]],
    max_nr_retries: int,
) -> Callable[[A], Awaitable[Try[B]]]:
    """
    Retry wrapper for async function A -> Try[B].

    Returns async function with same signature that executes input function at most
    `max_nr_retries` times, and returns either the first successful return value, or
    the last return value (which may be success or failure).

    Execution is logged to OpenTelemetry spans and iteration parameters are set as
    baggage.
    """
    assert max_nr_retries > 0

    async def do_retries(a: A) -> Try[B]:
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("retry-wrapper") as top_span:
            otel_add_baggage("max_nr_retries", max_nr_retries)
            top_span.set_attribute("max_nr_retries", max_nr_retries)

            for retry_nr in range(max_nr_retries):
                with tracer.start_as_current_span("retry-call") as iteration_span:
                    otel_add_baggage("retry_nr", retry_nr)
                    iteration_span.set_attribute("retry_nr", retry_nr)

                    try_b: Try[B] = await f(a)

                    # Note: status not set for iteration_span for now

                    if try_b.is_success():
                        top_span.set_status(Status(StatusCode.OK))
                        return try_b

            top_span.set_status(
                Status(
                    StatusCode.ERROR,
                    f"Function called retried {max_nr_retries} times; all failed!",
                )
            )
            return try_b

    return do_retries

    # ------------------------
    # Strangely (!) this function will fail with a 44 != 45 error in the test
    # test__task_ot__task_orchestration__run_three_tasks_in_sequence if last line is
    # replaced with the below (although the code should be equivalent?)
    #
    # @ray.remote(num_cpus=0)
    # class RetryActor:
    #     async def call(self, a: A) -> Try[B]:
    #         return await do_retries(a)
    #
    # retry_actor = RetryActor.remote()  # type: ignore
    # return lambda a: retry_actor.call.remote(a)
    #
    # Note also: without with the last lambda (which is not logically needed) we get
    # errors like: "TypeError: cannot create weak reference to 'NoneType' object"
    # Maybe the last lambda hides some metadata/attributes attached to the function,
    # and this leads to the wrong result.
    #
    # Alternatively, the below works:
    #
    # fut = Future.lift_async(do_retries)
    # return compose(fut, ray.put)
    # ------------------------
