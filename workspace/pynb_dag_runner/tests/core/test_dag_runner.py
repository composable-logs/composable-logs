import time, random
from typing import List, Set, Dict, Tuple, Optional, Any, Callable

#
import pytest, ray

#
from pynb_dag_runner.core.dag_runner import (
    Task,
    Task_OT,
    task_from_func,
    task_from_remote_f,
    TaskOutcome,
    TaskDependencies,
    in_sequence,
    in_parallel,
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


def test__make_task_from_remote_function__success():
    @ray.remote(num_cpus=0)
    def f():
        return 1234

    def g():
        return 1234

    for t in [task_from_remote_f(f.remote), task_from_func(g)]:
        t.start()

        result = ray.get(t.get_ref())

        assert result.return_value == 1234
        assert result.error is None


def test__make_task_from_function__fail():
    def f():
        raise Exception("kaboom!")

    task_f = task_from_func(f)
    task_f.start()

    result = ray.get(task_f.get_ref())

    assert result.return_value is None
    assert "kaboom!" in str(result.error)


def test__task_orchestration__run_three_tasks_in_sequence():
    def f():
        return 43

    def g(arg):
        assert isinstance(arg, TaskOutcome)
        assert arg.error is None
        assert arg.return_value == 43
        return arg.return_value + 1

    def h(arg):
        assert isinstance(arg, TaskOutcome)
        assert arg.error is None
        assert arg.return_value == 44
        return arg.return_value + 1

    all_tasks = in_sequence(*[task_from_func(_f) for _f in [f, g, h]])
    all_tasks.start()

    outcome = ray.get(all_tasks.get_ref())
    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert outcome.return_value == 45


def test__task_orchestration__run_three_tasks_in_parallel__failed():
    def f(*args):
        return 1234

    def g(*args):
        raise Exception("Exception from g")

    def h(*args):
        return 123

    combined_task = in_parallel(*[task_from_func(_f) for _f in [f, g, h]])

    combined_task.start()
    outcome = ray.get(combined_task.get_ref())

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is not None
    assert [o.return_value for o in outcome.return_value] == [1234, None, 123]


def test__task_orchestration__run_three_tasks_in_parallel__success():
    def f(*args):
        return 1234

    def g(*args):
        return 123

    combined_task = in_parallel(*[task_from_func(_f) for _f in [f, g]])

    combined_task.start()
    outcome = ray.get(combined_task.get_ref())

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert [o.return_value for o in outcome.return_value] == [1234, 123]
