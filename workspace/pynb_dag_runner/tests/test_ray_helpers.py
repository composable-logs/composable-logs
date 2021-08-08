import time
from pathlib import Path
from uuid import uuid4
from typing import Callable

#
import pytest, ray

#
from pynb_dag_runner.ray_helpers import try_eval_f_async_wrapper, Future


@ray.remote(num_cpus=0)
class StateActor:
    def __init__(self):
        self._state = []

    def add(self, value):
        self._state += [value]

    def get(self):
        return self._state


def test_future_map():
    @ray.remote(num_cpus=0)
    def f() -> int:
        return 123

    # example of a future having Future[int] type, but type checker does not notice
    # any problem with the below code.
    future: Future[bool] = f.remote()

    assert ray.get(Future.map(future, lambda x: x + 1)) == 124


### tests for try_eval_f_async_wrapper wrapper


def test_timeout_w_success():
    def f(x: int) -> int:
        return x + 1

    f_timeout: Callable[[Future[int]], Future[int]] = try_eval_f_async_wrapper(
        f,
        timeout_s=10,
        success_handler=lambda x: 2 * x,
        error_handler=lambda _: None,
    )

    for x in range(3):
        assert ray.get(f_timeout(ray.put(x))) == 2 * (x + 1)


def test_timeout_w_exception():
    def f(dummy):
        raise Exception(f"BOOM{dummy}")

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=10,
        success_handler=lambda _: None,
        error_handler=lambda x: x,
    )

    for x in range(3):
        try:
            _ = ray.get(f_timeout(ray.put(x)))
        except Exception as e:
            assert f"BOOM{x}" in str(e)


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
