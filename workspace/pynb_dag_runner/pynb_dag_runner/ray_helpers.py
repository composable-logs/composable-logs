from typing import TypeVar, Generic, Callable, List, Optional

import ray


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


def timeout_guard(
    f: Callable[[A], C],
    timeout_result: C,
    timeout_s: Optional[float],
) -> Callable[[Future[A]], Future[C]]:
    # See Ray issues
    # - "Support timeout option in Ray tasks",
    #   https://github.com/ray-project/ray/issues/17451
    # - "Set time-out on individual ray task"
    #   https://github.com/ray-project/ray/issues/15672

    @ray.remote(num_cpus=0)
    class ExecActor:
        def __init__(self, f):
            self.f = f

        def make_call(self, *args):
            # TODO: I/O in FunctionExecution-span
            return self.f(*args)

    def result(arg_f: Future[A]) -> Future[C]:
        # TODO: TimeoutGuard-span for below
        exec_actor = ExecActor.options(max_concurrency=2).remote(f)  # type: ignore

        future = exec_actor.make_call.remote(arg_f)

        refs_done, refs_not_done = ray.wait([future], num_returns=1, timeout=timeout_s)
        if len(refs_done) == 1:
            assert refs_done == [future]
            return future
        else:
            assert refs_not_done == [future]

            # https://docs.ray.io/en/latest/actors.html#terminating-actors
            ray.kill(exec_actor)

            return ray.put(timeout_result)

    return result


def try_eval_f_async_wrapper(
    f: Callable[[A], B],
    timeout_s: Optional[float],
    success_handler: Callable[[B], C],
    error_handler: Callable[[Exception], C],
) -> Callable[[Future[A]], Future[C]]:
    def tryf(*args) -> C:
        try:
            return success_handler(f(*args))
        except Exception as e:
            return error_handler(e)

    return timeout_guard(
        f=tryf,
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
