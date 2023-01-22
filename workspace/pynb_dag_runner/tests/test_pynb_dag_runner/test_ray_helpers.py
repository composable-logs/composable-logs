import time
from typing import TypeVar, Optional

#
import pytest, ray

#
from pynb_dag_runner.helpers import Success, Failure, one, Try, del_key
from pynb_dag_runner.wrappers import timeout_guard_wrapper
from pynb_dag_runner.opentelemetry_helpers import SpanDict, read_key, SpanRecorder


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


@pytest.mark.parametrize("timeout_s", [None, 10.0])
def test_timeout_with_success(timeout_s: Optional[float]):
    def get_spans():
        def f(x, y, z):
            assert z == "z"
            return x + y

        timeout_f = timeout_guard_wrapper(f, timeout_s=timeout_s, num_cpus=1)

        with SpanRecorder() as rec:
            assert timeout_f(1, 2, z="z") == Success(1 + 2)

        return rec.spans

    for span in [one(get_spans().filter(["name"], "call-python-function"))]:
        assert read_key(span, ["status", "status_code"]) == "OK"


@pytest.mark.parametrize("timeout_s", [None, 10.0])
def test_timeout_with_function_failure(timeout_s: Optional[float]):
    test_exception = ValueError("The test function f failed")

    def get_spans():
        def f():
            raise test_exception

        timeout_f = timeout_guard_wrapper(f, timeout_s=timeout_s, num_cpus=1)

        with SpanRecorder() as rec:
            assert timeout_f() == Failure(test_exception)

        return rec.spans

    for span in [one(get_spans().filter(["name"], "call-python-function"))]:
        assert span["status"] == {"status_code": "ERROR", "description": "Failure"}

        for event in [one(read_key(span, ["events"]))]:
            assert del_key(event, "timestamp", strict=True) == {
                "attributes": {
                    "exception.escaped": "False",
                    "exception.message": "The test function f failed",
                    "exception.stacktrace": "NoneType: None\n",
                    "exception.type": "ValueError",
                },
                "name": "exception",
            }


def test_timeout_with_stuck_function():
    def get_spans():
        def f():
            time.sleep(1e6)

        timeout_f = timeout_guard_wrapper(f, timeout_s=1.0, num_cpus=1)

        with SpanRecorder() as rec:
            assert timeout_f() == Failure(
                Exception(
                    "Timeout error: execution did not finish within timeout limit"
                )
            )

        return rec.spans

    for span in [one(get_spans().filter(["name"], "call-python-function"))]:
        assert span["status"] == {"status_code": "ERROR", "description": "Failure"}

        for event in [one(read_key(span, ["events"]))]:
            assert del_key(event, "timestamp", strict=True) == {
                "attributes": {
                    "exception.escaped": "False",
                    "exception.message": "Timeout error: execution did not finish within timeout limit",
                    "exception.stacktrace": "NoneType: None\n",
                    "exception.type": "Exception",
                },
                "name": "exception",
            }


# earlier versions of this test (w earlier versions of Ray) has failed randomly
@pytest.mark.parametrize("dummy_loop_parameter", range(1))
@pytest.mark.parametrize("timeout_s", [0.001, 10.0])
def test_timeout_w_timeout(dummy_loop_parameter: int, timeout_s: float):
    state_actor = StateActor.remote()  # type: ignore

    task_duration_s = 0.5

    def f() -> int:
        time.sleep(task_duration_s)

        state_actor.add.remote("f not killed by timeout")
        return 123

    # Wait for wrapped task to finish.
    result = timeout_guard_wrapper(f, timeout_s=timeout_s, num_cpus=1)()

    if timeout_s < task_duration_s:
        # f should have been timed out, and f should not have finished executing
        assert result == Failure(
            Exception("Timeout error: execution did not finish within timeout limit")
        )
        assert ray.get(state_actor.get.remote()) == []
    else:
        # f had time to fully execute
        assert result == Success(123)
        assert ray.get(state_actor.get.remote()) == ["f not killed by timeout"]


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
