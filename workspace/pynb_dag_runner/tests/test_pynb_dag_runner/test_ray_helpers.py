import time
from typing import Any, Awaitable, Callable, TypeVar

#
import pytest, ray

#
from pynb_dag_runner.helpers import Success, Failure, one, Try
from pynb_dag_runner.ray_helpers import try_f_with_timeout_guard
from pynb_dag_runner.opentelemetry_helpers import (
    SpanDict,
    read_key,
    Spans,
    SpanRecorder,
)


A = TypeVar("A")


@ray.remote(num_cpus=0)
class StateActor:
    def __init__(self):
        self._state = []

    def add(self, value):
        self._state += [value]

    def get(self):
        return self._state


### --- tests for try_f_with_timeout_guard wrapper ---


@pytest.mark.asyncio
async def test_timeout_w_success():
    N_calls = 3

    async def get_test_spans():
        with SpanRecorder() as rec:

            def f(x: int) -> int:
                return x + 1

            f_timeout: Callable[[int], Awaitable[Try[int]]] = try_f_with_timeout_guard(
                f, timeout_s=10, num_cpus=1
            )

            for x in range(N_calls):
                assert await f_timeout(x) == Try(x + 1, None)

        return rec.spans

    def validate_spans(spans: Spans):
        func_call_spans: Spans = spans.filter(["name"], "call-python-function")
        assert len(func_call_spans) == N_calls

        for span in func_call_spans:
            assert read_key(span, ["status", "status_code"]) == "OK"

    validate_spans(await get_test_spans())


@pytest.mark.asyncio
async def test_timeout_w_exception():
    N_calls = 3

    async def get_test_spans():
        with SpanRecorder() as rec:

            def error(dummy: int) -> Exception:
                return ValueError(f"BOOM{dummy}")

            def f(dummy: int):
                raise error(dummy)

            f_timeout: Callable[[int], Awaitable[Try[int]]] = try_f_with_timeout_guard(
                f, timeout_s=10, num_cpus=1
            )

            for x in range(N_calls):
                assert await f_timeout(x) == Try(None, error(x))
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

    validate_spans(await get_test_spans())


@pytest.mark.asyncio
async def test_timeout_w_timeout_cancel():
    N_calls = 3

    async def get_test_spans():
        with SpanRecorder() as rec:

            def f(_: str) -> int:
                time.sleep(1e6)
                return 123

            f_timeout: Callable[[str], Awaitable[Try[int]]] = try_f_with_timeout_guard(
                f, timeout_s=0.5, num_cpus=1
            )

            for _ in range(N_calls):
                result = await f_timeout("argument-to-function-f")
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
            assert read_key(span, ["attributes", "task.timeout_s"]) == 0.5
            assert span["status"] == {"status_code": "ERROR", "description": "Timeout"}

    validate_spans(await get_test_spans())


# this test has failed randomly (TODO)
@pytest.mark.asyncio
@pytest.mark.parametrize("dummy_loop_parameter", range(1))
@pytest.mark.parametrize("task_timeout_s", [0.001, 10.0])
async def test_timeout_w_timeout(dummy_loop_parameter, task_timeout_s):
    state_actor = StateActor.remote()  # type: ignore

    task_duration_s = 0.2

    def f(_: Any) -> int:
        time.sleep(task_duration_s)

        # We should not get here *if* task is canceled by timeout
        state_actor.add.remote("foo")
        return 123

    f_timeout: Callable[[Any], Awaitable[Try[int]]] = try_f_with_timeout_guard(
        f, timeout_s=task_timeout_s, num_cpus=1
    )

    result: Try[int] = await f_timeout("dummy")

    # Wait for task to finish
    time.sleep(4.0)

    state_has_flipped: bool = "foo" in await state_actor.get.remote()

    if task_timeout_s < task_duration_s:
        # f should have been canceled, and state should not have flipped
        assert not state_has_flipped
        assert "timeout" in str(result.error)
    else:
        assert state_has_flipped
        assert result == Try(123, None)


### ---- test Try implementation ----


def test_try_both_value_and_error_can_not_be_set():
    with pytest.raises(Exception):
        assert Try(1, Exception("foo"))


def test_try_is_success_method():
    assert Try(None, None).is_success() == True
    assert Try(12345, None).is_success() == True
    assert Try(None, Exception("Foo")).is_success() == False


def test_try_equality_checking():
    # wrapped None value
    assert Try(None, None) == Try(None, None) == Success(None)

    # wrapped non-None value
    assert Try(12345, None) == Success(12345)

    for not_12345 in [Success(1), Success(None), Failure(Exception("12")), None, 12345]:
        assert Try(12345, None) != not_12345

    for e1 in ["foo", "bar"]:
        assert Success(e1) != Failure(Exception(e1))

        for e2 in ["foo", "bar"]:
            if e1 == e2:
                assert Failure(Exception(e1)) == Failure(Exception(e2))
            else:
                assert Failure(Exception(e1)) != Failure(Exception(e2))


def test_try_map_value():
    assert Try(None, None).map_value(lambda x: x) == Try(None, None)
    assert Try(1, None).map_value(lambda x: x + 1) == Try(2, None)
    assert Try(None, Exception("foo")).map_value(lambda x: x) == Try(
        None, Exception("foo")
    )


def test_try_wrap():
    def f(x, y, z):
        assert z == "z"
        if x == y:
            return x + y
        else:
            raise Exception(f"x!=y, got x={x} and y={y}")

    assert Try.wrap(f)(1, 1, z="z") == Success(2)

    assert Try.wrap(f)(1, 2, z="z") == Failure(Exception(f"x!=y, got x=1 and y=2"))
