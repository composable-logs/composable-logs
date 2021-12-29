import time, random
from pathlib import Path
from uuid import uuid4
from typing import Any, Callable

#
import opentelemetry as otel
import pytest, ray

#
from pynb_dag_runner.helpers import A, flatten, range_intersect, one, Try
from pynb_dag_runner.ray_helpers import (
    try_f_with_timeout_guard,
    retry_wrapper,
    retry_wrapper_ot,
    Future,
    FutureActor,
)
from pynb_dag_runner.opentelemetry_helpers import read_key, Spans, SpanRecorder


@ray.remote(num_cpus=0)
class StateActor:
    def __init__(self):
        self._state = []

    def add(self, value):
        self._state += [value]

    def get(self):
        return self._state


### Test FutureActor


def test_future_actor():
    future_value = FutureActor.remote()

    future_value.set_value.remote("foo")

    for _ in range(10):
        assert ray.get(future_value.wait.remote()) == "foo"


@pytest.mark.asyncio
async def test_future_actor_async():
    future_value = FutureActor.remote()

    future_value.set_value.remote("bar")

    for _ in range(10):
        assert (await future_value.wait.remote()) == "bar"


### Test Future static functions


def test_future_value():
    assert ray.get(Future.value(42)) == 42


def test_future_map():
    @ray.remote(num_cpus=0)
    def f() -> int:
        return 123

    # example of a future having Future[int] type, but type checker does not notice
    # any problem with the below code.
    future: Future[bool] = f.remote()

    assert ray.get(Future.map(future, lambda x: x + 1)) == 124


def test_future_lift():
    assert ray.get(Future.lift(lambda x: x + 1)(Future.value(1))) == 2


def test_future_async_lift():
    async def f(x):
        return x + 1

    assert ray.get(Future.lift_async(f)(ray.put(1))) == 2


@pytest.mark.asyncio
async def test_future_async_lift_w_exception():
    async def f(_):
        raise Exception("boom!")

    with pytest.raises(Exception):
        await Future.lift_async(f)("dummy arg to f")

    with pytest.raises(Exception):
        ray.get(Future.lift_async(f)("dummy arg to f"))


### --- tests for try_f_with_timeout_guard wrapper ---


def test_timeout_w_success():
    N_calls = 3

    def get_test_spans():
        with SpanRecorder() as rec:

            def f(x: int) -> int:
                return x + 1

            f_timeout: Callable[
                [Future[int]], Future[Try[int]]
            ] = try_f_with_timeout_guard(f, timeout_s=10, num_cpus=1)

            for x in range(N_calls):
                assert ray.get(f_timeout(ray.put(x))) == Try(x + 1, None)

        return rec.spans

    def validate_spans(spans: Spans):
        func_call_spans: Spans = spans.filter(["name"], "call-python-function")
        assert len(func_call_spans) == N_calls

        for span in func_call_spans:
            assert read_key(span, ["status", "status_code"]) == "OK"

    validate_spans(get_test_spans())


def test_timeout_w_exception():
    N_calls = 3

    def get_test_spans():
        with SpanRecorder() as rec:

            def error(dummy: int) -> Exception:
                return ValueError(f"BOOM{dummy}")

            def f(dummy: int):
                raise error(dummy)

            f_timeout: Callable[
                [Future[int]], Future[Try[int]]
            ] = try_f_with_timeout_guard(f, timeout_s=10, num_cpus=1)

            for x in range(N_calls):
                assert ray.get(f_timeout(ray.put(x))) == Try(None, error(x))
        return rec.spans

    def validate_spans(spans: Spans):
        func_call_spans: Spans = spans.filter(["name"], "call-python-function")
        assert len(func_call_spans) == N_calls

        for span in func_call_spans:
            assert span["status"] == {"status_code": "ERROR", "description": "Failure"}

            event = one(read_key(span, ["events"]))
            assert set(event.keys()) == set(["name", "timestamp", "attributes"])
            assert event["name"] == "exception"
            assert set(event["attributes"]) == set(
                [
                    "exception.type",
                    "exception.message",
                    "exception.stacktrace",
                    "exception.escaped",
                ]
            )
            assert read_key(event, ["attributes", "exception.type"]) == "ValueError"

    validate_spans(get_test_spans())


def test_timeout_w_timeout_cancel():
    N_calls = 3

    def get_test_spans():
        with SpanRecorder() as rec:

            def f(_: Any) -> None:
                time.sleep(1e6)

            f_timeout: Callable[
                [Future[int]], Future[Try[int]]
            ] = try_f_with_timeout_guard(f, timeout_s=0.5, num_cpus=1)

            for _ in range(N_calls):
                result = ray.get(f_timeout(ray.put(None)))
                assert result == Try(
                    value=None,
                    error=Exception(
                        "Timeout error: execution did not finish within timeout limit"
                    ),
                )

        return rec.spans

    def validate_spans(spans: Spans):
        func_call_spans: Spans = spans.filter(["name"], "timeout-guard")
        assert len(func_call_spans) == N_calls

        for span in func_call_spans:
            assert read_key(span, ["attributes", "timeout_s"]) == 0.5
            assert span["status"] == {"status_code": "ERROR", "description": "Timeout"}

    validate_spans(get_test_spans())


# this test has failed randomly (TODO)
@pytest.mark.parametrize("dummy_loop_parameter", range(1))
@pytest.mark.parametrize("task_timeout_s", [0.001, 10.0])
def test_timeout_w_timeout(dummy_loop_parameter, task_timeout_s):
    state_actor = FutureActor.remote()

    task_duration_s = 0.2

    def f(_: Any) -> int:
        time.sleep(task_duration_s)

        # We should not get here *if* task is canceled by timeout
        state_actor.set_value.remote("foo")
        return 123

    f_timeout: Callable[[Future[Any]], Future[Try[int]]] = try_f_with_timeout_guard(
        f, timeout_s=task_timeout_s, num_cpus=1
    )

    result: Try[int] = ray.get(f_timeout(ray.put("dummy")))

    # Wait for task to finish
    time.sleep(4.0)

    state_has_flipped: bool = ray.get(state_actor.value_is_set.remote())

    if task_timeout_s < task_duration_s:
        # f should have been canceled, and state should not have flipped
        assert not state_has_flipped
        assert "timeout" in str(result.error)
    else:
        assert state_has_flipped
        assert result == Try(123, None)


### ---- tests for retry_wrapper ----


@pytest.mark.parametrize(
    "is_success", [lambda _: True, lambda _: False, lambda retry_nr: retry_nr >= 4]
)
def test_retry_constant_should_succeed(is_success):
    N_max_retries = 10
    retry_arguments = list(range(N_max_retries))
    any_success = any(map(is_success, retry_arguments))

    def get_test_spans():
        with SpanRecorder() as rec:
            f_result = retry_wrapper_ot(
                f_task_remote=ray.remote(num_cpus=0)(is_success).remote,
                retry_arguments=retry_arguments,
            )
            assert ray.get(f_result) == any_success

        return rec.spans

    def validate_spans(spans: Spans):
        retry_span = one(spans.filter(["name"], "retry-wrapper"))

        if any_success:
            assert read_key(retry_span, ["status", "status_code"]) == "OK"
        else:
            assert retry_span["status"] == {
                "description": f"Task retried {N_max_retries} times; all failed!",
                "status_code": "ERROR",
            }

    validate_spans(get_test_spans())


### ---- test Try implementation ----


def test_try_both_value_and_error_can_not_be_set():
    with pytest.raises(Exception):
        assert Try(1, Exception("foo"))


def test_try_equality_checking():
    assert Try(None, None) == Try(None, None)

    assert Try(12345, None) == Try(12345, None)
    assert Try(12345, None) != Try(None, None)

    assert Try(None, Exception("foo")) == Try(None, Exception("foo"))
    assert Try(None, Exception("foo")) != Try(None, Exception("bar"))

    assert Try(123, None) != Exception("!!!")
    assert Try(123, None) != (lambda: None)
