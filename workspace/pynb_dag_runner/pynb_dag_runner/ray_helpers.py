from typing import TypeVar, Generic, Callable, List, Optional

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
    def lift(f: Callable[[B], C]) -> "Callable[[Future[B]], Future[C]]":
        return lambda future: Future.map(future, f)


@ray.remote(num_cpus=0)
class LiftedFunctionActor:
    def __init__(self, f, success_handler, error_handler):
        self.f = f
        self.success_handler = success_handler
        self.error_handler = error_handler

    def call(self, *args, **kwargs):
        tracer = otel.trace.get_tracer(__name__)

        # Execute function in new OpenTelemetry span.
        # Note that arguments to function are not logged.
        with tracer.start_as_current_span("call-python-function") as span:
            try:
                result = self.success_handler(self.f(*args, **kwargs))
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                result = self.error_handler(e)
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, "Failure"))

            span.set_attribute("return_value", str(result))

            return result


def timeout_guard(
    make_actor,
    timeout_result: C,
    timeout_s: Optional[float],
) -> Callable[[Future[A]], Future[C]]:
    """
    Note we are passing in a lambda for creating actor; not the actor itself. Otherwise
    we get errors in unit tests like: "ray.exceptions.RayActorError: The actor died
    unexpectedly before finishing this task".

    Maybe this ensures the correct hierarchy between actors (relevant when killing
    actors)?

    See also Ray issues:
     - "Support timeout option in Ray tasks",
       https://github.com/ray-project/ray/issues/17451
     - "Set time-out on individual ray task"
       https://github.com/ray-project/ray/issues/15672
    """

    def compute_after_dependency_ready(a: A) -> C:
        tracer = otel.trace.get_tracer(__name__)
        with tracer.start_as_current_span("call-python-function-x") as span:
            span.set_attribute("timeout_s", timeout_s)
            work_actor = make_actor()
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

                return timeout_result

    return Future.lift(compute_after_dependency_ready)


def try_eval_f_async_wrapper(
    f: Callable[[A], B],
    timeout_s: Optional[float],
    success_handler: Callable[[B], C],
    error_handler: Callable[[Exception], C],
) -> Callable[[Future[A]], Future[C]]:
    return timeout_guard(
        make_actor=lambda: LiftedFunctionActor.remote(  # type: ignore
            f, success_handler, error_handler  # type: ignore
        ),  # type: ignore
        timeout_result=error_handler(
            Exception("Timeout error: execution did not finish within timeout limit")
        ),
        timeout_s=timeout_s,
    )


RetryCount = int


def retry_wrapper(
    f_task_remote: Callable[[RetryCount], Future[A]],
    max_retries: RetryCount,
    is_success: Callable[[A], bool],
) -> Future[List[A]]:
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
