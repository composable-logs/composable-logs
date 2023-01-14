from typing import TypeVar, Callable, Optional, Awaitable

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
            # TODO: Review what happens here if timeout is None.
            # Does set_attribute/add baggage support that?
            span.set_attribute("task.timeout_s", timeout_s)  # type: ignore
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
