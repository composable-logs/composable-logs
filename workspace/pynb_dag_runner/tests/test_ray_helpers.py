import time, random
from pathlib import Path
from uuid import uuid4
from typing import Any, Callable

#
import opentelemetry as otel
import pytest, ray

#
from pynb_dag_runner.helpers import flatten, range_intersect, one
from pynb_dag_runner.ray_helpers import (
    try_eval_f_async_wrapper,
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


### tests for try_eval_f_async_wrapper wrapper


def test_timeout_w_success():
    N_calls = 3

    def get_test_spans():
        with SpanRecorder() as rec:

            def f(x: int) -> int:
                return x + 1

            f_timeout: Callable[[Future[int]], Future[int]] = try_eval_f_async_wrapper(
                f,
                timeout_s=10,
                success_handler=lambda x: 2 * x,
                error_handler=lambda _: None,
            )

            for x in range(N_calls):
                assert ray.get(f_timeout(ray.put(x))) == 2 * (x + 1)

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

            def f(dummy):
                raise ValueError(f"BOOM{dummy}")

            f_timeout = try_eval_f_async_wrapper(
                f,
                timeout_s=10,
                success_handler=lambda _: None,
                error_handler=lambda x: x,
            )

            for x in range(N_calls):
                try:
                    _ = ray.get(f_timeout(ray.put(x)))
                except ValueError as e:
                    assert f"BOOM{x}" in str(e)
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

            f_timeout: Callable[[Future[Any]], Future[Any]] = try_eval_f_async_wrapper(
                f,
                timeout_s=0.5,
                success_handler=lambda _: "OK",
                error_handler=lambda e: "FAIL:" + str(e),
            )

            for _ in range(N_calls):
                assert "FAIL:" in ray.get(f_timeout(ray.put(None)))

        return rec.spans

    def validate_spans(spans: Spans):
        func_call_spans: Spans = spans.filter(["name"], "timeout-guard")
        assert len(func_call_spans) == N_calls

        for span in func_call_spans:
            assert read_key(span, ["attributes", "timeout_s"]) == 0.5
            assert span["status"] == {"status_code": "ERROR", "description": "Timeout"}

    validate_spans(get_test_spans())


def test_logging_for_nested_lift_functions():
    N_calls = 3

    def get_test_spans():
        with SpanRecorder() as rec:

            def f(x):
                time.sleep(0.05)
                return x + 123

            f_inner: Callable[[Future[int]], Future[int]] = try_eval_f_async_wrapper(
                f=f,
                timeout_s=5,
                success_handler=lambda x: {"inner": x},
                error_handler=lambda e: "FAIL:" + str(e),
            )

            f_outer: Callable[[Future[int]], Future[int]] = try_eval_f_async_wrapper(
                f=lambda x: ray.get(f_inner(ray.put(x))),
                timeout_s=5,
                success_handler=lambda x: {"outer": x},
                error_handler=lambda e: "FAIL:" + str(e),
            )

            for x in range(N_calls):
                tracer = otel.trace.get_tracer(__name__)
                with tracer.start_as_current_span(f"top-{x}") as t:
                    assert ray.get(f_outer(ray.put(x))) == {"outer": {"inner": 123 + x}}

        return rec.spans

    def validate_spans(spans: Spans):
        for x in range(N_calls):
            top_x = one(spans.filter(["name"], f"top-{x}"))

            spans_under_top_x: Spans = spans.restrict_by_top(top_x)

            g1, g2 = list(
                spans_under_top_x.filter(["name"], "timeout-guard").sort_by_start_time()
            )
            c1, c2 = list(
                spans_under_top_x.filter(
                    ["name"], "call-python-function"
                ).sort_by_start_time()
            )

            # log also contain spans generated by Ray for actor and function calls
            assert len(spans_under_top_x) > 4

            # Check span inclusions: g1 <- c1 <- g2 <- c2:
            assert spans_under_top_x.contains_path(g1, c1, g2, c2)

    validate_spans(get_test_spans())


# this test has failed randomly (TODO)
@pytest.mark.parametrize("dummy_loop_parameter", range(1))
@pytest.mark.parametrize("task_timeout_s", [0.001, 10.0])
@pytest.mark.parametrize("state_type", ["Actor", "File"])
def test_timeout_w_timeout(
    tmp_path: Path, dummy_loop_parameter, state_type, task_timeout_s
):
    class State:
        pass

    class FileState(State):
        def __init__(self):
            self.temp_file = tmp_path / f"{uuid4()}.txt"

        def flip(self):
            return self.temp_file.touch()

        def did_flip(self) -> bool:
            return self.temp_file.is_file()

    class ActorState(State):
        def __init__(self):
            self.state_actor = StateActor.remote()

        def flip(self):
            return self.state_actor.add.remote(1)

        def did_flip(self) -> bool:
            return 1 in ray.get(self.state_actor.get.remote())

    assert state_type in ["Actor", "File"]
    state: State = ActorState() if state_type == "Actor" else FileState()

    task_duration_s = 0.2

    def f(dummy):
        time.sleep(task_duration_s)

        # We should not get here if the task is canceled by timeout
        state.flip()

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=task_timeout_s,
        success_handler=lambda _: "RUN OK",
        error_handler=lambda e: "FAIL:" + str(e),
    )

    result = ray.get(f_timeout(ray.put("dummy")))

    # Wait for task to finish
    time.sleep(4.0)

    if task_timeout_s < task_duration_s:
        # f should have been canceled, and state should not have flipped
        assert not state.did_flip()  # type: ignore
        assert result.startswith("FAIL:") and "timeout" in result.lower()
        assert "timeout" in result.lower()
    else:
        assert state.did_flip()  # type: ignore
        assert result == "RUN OK"


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
