import time, random, itertools
from pathlib import Path
from typing import List

#
import pytest

#
from pynb_dag_runner.tasks.tasks import PythonFunctionTask, get_task_dependencies

#
from pynb_dag_runner.helpers import (
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
    read_json,
)
from pynb_dag_runner.core.dag_runner import (
    TaskDependencies,
    run_tasks,
)
from pynb_dag_runner.wrappers.runlog import Runlog

## TODO: all the below tests should run multiple times for stress testing


### ---- test order dependence for PythonFunctionTask:s ----


def assert_compatibility(runlog_results: List[Runlog], task_id_dependencies):
    """
    Test:
     - generic invariances for runlog timings (Steps 1, 2)
     - order constraints in dependency DAG are satisfied by output timings (Step 3)
    """

    # Step 1: all task-id:s in order dependencies must have at least one runlog
    # entry. (The converse need not hold.)
    task_ids_in_runlog: List[str] = [runlog["task_id"] for runlog in runlog_results]
    task_ids_in_dependencies: List[str] = [d["from"] for d in task_id_dependencies] + [
        d["to"] for d in task_id_dependencies
    ]
    assert set(task_ids_in_dependencies) <= set(task_ids_in_runlog)

    # Step 2: A task retry should not start before previous attempt for running task
    # has finished.
    def get_runlogs(task_id):
        return [runlog for runlog in runlog_results if runlog["task_id"] == task_id]

    for task_id in task_ids_in_runlog:
        task_id_retries = get_runlogs(task_id)
        for idx, _ in enumerate(task_id_retries):
            if idx > 0:
                assert (
                    task_id_retries[idx - 1]["out.timing.end_ts"]
                    < task_id_retries[idx]["out.timing.start_ts"]
                )

    # Step 3: Runlog timings satisfy the same order constraints as in DAG run order
    # dependencies.
    for rule in task_id_dependencies:
        ts0 = max([runlog["out.timing.end_ts"] for runlog in get_runlogs(rule["from"])])
        ts1 = min([runlog["out.timing.start_ts"] for runlog in get_runlogs(rule["to"])])
        assert ts0 < ts1


### ---- Tests for get_task_dependencies ----


def test_get_task_dependencies():
    assert len(get_task_dependencies(TaskDependencies())) == 0

    t0 = PythonFunctionTask(f=lambda: None, task_id="t0")
    t1 = PythonFunctionTask(f=lambda: None, task_id="t1")
    t2 = PythonFunctionTask(f=lambda: None, task_id="t2")
    t3 = PythonFunctionTask(f=lambda: None, task_id="t3")

    assert get_task_dependencies((t0 >> t1 >> t2) + TaskDependencies(t1 >> t3)) == [
        {"from": "t0", "to": "t1"},
        {"from": "t1", "to": "t2"},
        {"from": "t1", "to": "t3"},
    ]


### ---- Test PythonFunctionTask evaluation ----


def test_tasks_runlog_output(tmp_path: Path):
    dependencies = TaskDependencies()
    runlog_results = flatten(
        run_tasks(
            [
                PythonFunctionTask(
                    lambda _: 123, get_run_path=lambda _: tmp_path, task_id="t1"
                ),
            ],
            dependencies,
        )
    )
    assert len(runlog_results) == 1

    assert runlog_results[0]["task_id"] == "t1"
    assert runlog_results[0]["out.result"] == 123
    assert runlog_results[0].keys() == set(
        [
            "task_id",
            "parameters.task.n_max_retries",
            "parameters.task.timeout_s",
            "parameters.run.retry_nr",
            "parameters.run.id",
            "parameters.run.run_directory",
            "out.timing.start_ts",
            "out.timing.end_ts",
            "out.timing.duration_ms",
            "out.status",
            "out.error",
            "out.result",
        ]
    )

    # assert that runlog json has been written to disk
    assert runlog_results[0].as_dict() == read_json(tmp_path / "runlog.json")

    assert_compatibility(runlog_results, get_task_dependencies(dependencies))


def test_tasks_run_in_parallel():
    dependencies = TaskDependencies()

    runlog_results = flatten(
        run_tasks(
            [
                PythonFunctionTask(lambda _: time.sleep(1.0), task_id="t0"),
                PythonFunctionTask(lambda _: time.sleep(1.0), task_id="t1"),
            ],
            dependencies,
        )
    )
    assert len(runlog_results) == 2

    # Check: since there are no order constraints, the the time ranges should
    # overlap provided tests are run on 2+ CPUs
    range1, range2 = [
        range(runlog["out.timing.start_ts"], runlog["out.timing.end_ts"])
        for runlog in runlog_results
    ]
    assert range_intersect(range1, range2)

    assert_compatibility(runlog_results, get_task_dependencies(dependencies))


def test_parallel_tasks_are_queued_based_on_available_ray_worker_cpus():
    start_ts = time.time_ns()

    dependencies = TaskDependencies()
    runlog_results = flatten(
        run_tasks(
            [
                PythonFunctionTask(lambda _: time.sleep(0.5), task_id="t0"),
                PythonFunctionTask(lambda _: time.sleep(0.5), task_id="t1"),
                PythonFunctionTask(lambda _: time.sleep(0.5), task_id="t2"),
                PythonFunctionTask(lambda _: time.sleep(0.5), task_id="t3"),
            ],
            dependencies,
        )
    )
    assert len(runlog_results) == 4

    end_ts = time.time_ns()

    # Check 1: with only 2 CPU:s running the above tasks with no constraints should
    # take > 1 secs.
    duration_ms = (end_ts - start_ts) // 1000000
    assert duration_ms >= 1000, duration_ms

    task_runtime_ranges = [
        range(runlog["out.timing.start_ts"], runlog["out.timing.end_ts"])
        for runlog in runlog_results
    ]

    # Check 2: if tasks are run on 2 CPU:s the intersection of three runtime ranges
    # should always be empty.
    for r1, r2, r3 in itertools.combinations(task_runtime_ranges, 3):
        assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))

    assert_compatibility(runlog_results, get_task_dependencies(dependencies))


def test_retry_logic_in_python_function_task():
    timeout_s = 5.0

    def f(runlog):
        if runlog["parameters.run.retry_nr"] == 0:
            time.sleep(1e6)  # hang execution to generate a timeout error
        elif runlog["parameters.run.retry_nr"] == 1:
            raise Exception("BOOM!")
        elif runlog["parameters.run.retry_nr"] == 2:
            return 123

    t0 = PythonFunctionTask(f, task_id="t0", timeout_s=timeout_s, n_max_retries=3)
    t1 = PythonFunctionTask(lambda _: 42, task_id="t1")
    dependencies = TaskDependencies(t0 >> t1)

    runlog_results = flatten(run_tasks([t0, t1], dependencies))
    assert len(runlog_results) == 4

    # All retries should have a distinct run-id:s
    assert len(set([r["parameters.run.id"] for r in runlog_results])) == 4

    # t0 should be run 3 times, t1 once
    assert [r["task_id"] for r in runlog_results] == ["t0", "t0", "t0", "t1"]

    # remove non-deterministic runlog keys, and assert remaining keys are as expected
    def del_keys(a_dict, keys_to_delete):
        return {k: v for k, v in a_dict.items() if k not in keys_to_delete}

    deterministic_runlog = [
        del_keys(
            r.as_dict(),
            keys_to_delete=[
                "parameters.run.id",
                "out.timing.duration_ms",
                "out.timing.start_ts",
                "out.timing.end_ts",
            ],
        )
        for r in runlog_results
    ]

    expected_det_runlog = [
        {
            "task_id": "t0",
            "parameters.task.timeout_s": timeout_s,
            "parameters.task.n_max_retries": 3,
            "parameters.run.retry_nr": 0,
            "out.status": "FAILURE",
            "out.result": None,
            "out.error": "Timeout error: execution did not finish within timeout limit",
        },
        {
            "task_id": "t0",
            "parameters.task.timeout_s": timeout_s,
            "parameters.task.n_max_retries": 3,
            "parameters.run.retry_nr": 1,
            "out.status": "FAILURE",
            "out.result": None,
            "out.error": "BOOM!",
        },
        {
            "task_id": "t0",
            "parameters.task.timeout_s": timeout_s,
            "parameters.task.n_max_retries": 3,
            "parameters.run.retry_nr": 2,
            "out.status": "SUCCESS",
            "out.result": 123,
            "out.error": None,
        },
        {
            "task_id": "t1",
            "parameters.task.timeout_s": None,
            "parameters.task.n_max_retries": 1,
            "parameters.run.retry_nr": 0,
            "out.status": "SUCCESS",
            "out.result": 42,
            "out.error": None,
        },
    ]

    assert deterministic_runlog == expected_det_runlog
    assert_compatibility(runlog_results, get_task_dependencies(dependencies))


@pytest.mark.parametrize(
    "dependencies_list",
    [
        [],
        #
        #  t0  --->  t1
        #
        ["t0 >> t1"],
        #
        #  t0  --->  t1
        #
        #  t2  --->  t3  ---> t4
        #
        ["t0 >> t1", "t2 >> t3", "t3 >> t4"],
        # same as above, but one constraint repeated
        ["t0 >> t1", "t2 >> t3", "t3 >> t4", "t3 >> t4"],
        #
        #  t0  --->  t1  ---\
        #                    v
        #  t2  --->  t3  ---> t4
        #
        ["t0 >> t1", "t1 >> t4", "t2 >> t3", "t3 >> t4"],
        #
        #      --->  t0  ---> t1
        #     /
        #  t2  --->  t3  ---> t4
        #
        ["t2 >> t0", "t2 >> t3", "t0 >> t1", "t3 >> t4"],
        #
        #  t0  --->  t1  --->  t2  --->  t3  ---> t4
        #
        ["t0 >> t1", "t1 >> t2", "t2 >> t3", "t3 >> t4"],
        #
        #       --->  t1  ---\
        #      /              v
        #  t0  ---->  t2  --->  t4
        #      \              ^
        #       --->  t3  ---/
        #
        [
            "t0 >> t1",
            "t0 >> t2",
            "t0 >> t3",
            "t1 >> t4",
            "t2 >> t4",
            "t3 >> t4",
        ],
        #
        #  t0  ---\
        #          v
        #            t1  ---\
        #          ^         v
        #  t2  ---/           t4
        #                    ^
        #  t3  -------------/
        #
        ["t0 >> t1", "t2 >> t1", "t1 >> t4", "t3 >> t4"],
        # same as above, and two redundant constraints
        ["t0 >> t1", "t2 >> t1", "t1 >> t4", "t3 >> t4", "t0 >> t4", "t2 >> t4"],
        #
        #  t0  --->  t1  --->  t2
        #      \          \ ^
        #       \          X
        #        \        / v
        #         >  t3  --->  t4
        #
        ["t0 >> t1", "t0 >> t3", "t1 >> t2", "t3 >> t4", "t1 >> t4", "t3 >> t2"],
    ],
)
def test_random_sleep_tasks_with_order_dependencies(dependencies_list):
    def sleep_f():
        sleep_ms = random.randint(10, 100)
        return lambda _: time.sleep(sleep_ms / 1000)

    t0 = PythonFunctionTask(sleep_f(), task_id="t0")
    t1 = PythonFunctionTask(sleep_f(), task_id="t1")
    t2 = PythonFunctionTask(sleep_f(), task_id="t2")
    t3 = PythonFunctionTask(sleep_f(), task_id="t3")
    t4 = PythonFunctionTask(sleep_f(), task_id="t4")

    # See https://stackoverflow.com/questions/55084171
    f_locals = locals()
    dependencies = TaskDependencies(*[eval(d, f_locals) for d in dependencies_list])
    runlog_results = flatten(run_tasks([t0, t1, t2, t3, t4], dependencies))
    assert len(runlog_results) == 5

    assert_compatibility(runlog_results, get_task_dependencies(dependencies))
