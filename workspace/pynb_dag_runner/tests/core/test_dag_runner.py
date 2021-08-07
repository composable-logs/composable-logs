import time, random
from typing import List, Set, Dict, Tuple, Optional, Any, Callable

#
import ray, pytest

#
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.core.dag_runner import (
    Task,
    TaskDependence,
    TaskDependencies,
    run_tasks,
)

ray.init(num_cpus=2, ignore_reinit_error=True)


def make_task(sleep_secs: float, return_value: int) -> Task[int]:
    @ray.remote(num_cpus=0)
    def f():
        time.sleep(sleep_secs)
        return return_value

    return Task(f.remote)


@pytest.mark.parametrize(
    "task_dependencies",
    [
        "[]",
        "[task0 >> task1]",
        "[task1 >> task0]",
        "[task1 >> task0, task1 >> task2]",
        "[task0 >> task1, task1 >> task2]",
        "[task0 >> task1, task1 >> task2, task0 >> task2]",
    ],
)
def test_all_tasks_are_run(task_dependencies):
    task0 = make_task(sleep_secs=0.05, return_value=0)
    task1 = make_task(sleep_secs=0.01, return_value=1)
    task2 = make_task(sleep_secs=0.025, return_value=2)

    result = run_tasks(
        [task0, task1, task2], TaskDependencies(*eval(task_dependencies))
    )
    assert len(result) == 3 and set(result) == set([0, 1, 2])


def test_task_run_order():
    @ray.remote(num_cpus=1)
    class GlobalState:
        def __init__(self):
            self.state = []

        def add(self, value):
            self.state += [value]

        def get(self):
            return self.state

    state_actor = GlobalState.remote()

    def make_task(i: int) -> Task[int]:
        @ray.remote(num_cpus=0)
        def f():
            time.sleep(random.random() * 0.10)
            state_actor.add.remote(i)

        return Task(f.remote)

    task0, task1, task2 = [make_task(i) for i in range(3)]

    _ = run_tasks(
        [task0, task1, task2], TaskDependencies(task1 >> task0, task2 >> task0)
    )

    state = ray.get(state_actor.get.remote())
    assert len(state) == 3

    # task0 should run last while run order of task1 and task2 is random
    assert state[0] in [1, 2]
    assert state[1] in [1, 2]
    assert state[2] == 0
