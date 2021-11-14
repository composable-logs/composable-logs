import time, random
from typing import List, Set, Dict, Tuple, Optional, Any, Callable

#
import pytest, ray

#
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.core.dag_runner import (
    Task,
    TaskDependence,
    TaskDependencies,
    in_sequence,
    run_tasks,
)
from tests.test_ray_helpers import StateActor


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
    def make_task(sleep_secs: float, return_value: int) -> Task[int]:
        @ray.remote(num_cpus=0)
        def f(_):
            time.sleep(sleep_secs)
            return return_value

        return Task(f.remote)

    task0 = make_task(sleep_secs=0.05, return_value=0)
    task1 = make_task(sleep_secs=0.01, return_value=1)
    task2 = make_task(sleep_secs=0.025, return_value=2)

    result = run_tasks(
        [task0, task1, task2], TaskDependencies(*eval(task_dependencies))
    )
    assert len(result) == 3 and set(result) == set([0, 1, 2])


@pytest.mark.parametrize("dummy_loop_parameter", range(1))
def test_task_run_order(dummy_loop_parameter):
    state_actor = StateActor.remote()

    def make_task(i: int) -> Task[int]:
        @ray.remote(num_cpus=0)
        def f(_):
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


def test__task_orchestration__run_three_tasks_in_sequence():
    @ray.remote(num_cpus=0)
    def f(arg):
        assert arg == 42
        return arg + 1

    @ray.remote(num_cpus=0)
    def g(arg):
        assert arg == 43
        return arg + 1

    @ray.remote(num_cpus=0)
    def h(arg):
        assert arg == 44
        return arg + 1

    all_tasks = in_sequence(Task(f.remote), Task(g.remote), Task(h.remote))
    all_tasks.start(ray.put(42))
    assert ray.get(all_tasks.get_ref()) == 45
