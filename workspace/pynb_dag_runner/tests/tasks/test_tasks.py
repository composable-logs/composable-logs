import time, itertools

#
from pynb_dag_runner.tasks.tasks import PythonTask

#
from pynb_dag_runner.helpers import (
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
)
from pynb_dag_runner.core.dag_runner import (
    TaskDependencies,
    run_tasks,
)

## TODO: all the below tests should run multiple times for stress testing


def test_tasks_run_in_parallel():
    runlog_results = flatten(
        run_tasks(
            [
                PythonTask(lambda _: time.sleep(1.0), task_id="t0"),
                PythonTask(lambda _: time.sleep(1.0), task_id="t1"),
            ],
            TaskDependencies(),
        )
    )

    # Check: since there are no order constraints, the the time ranges should
    # overlap provided tests are run on 2+ CPUs
    range1, range2 = [
        range(runlog["out.timing.start_ts"], runlog["out.timing.end_ts"])
        for runlog in runlog_results
    ]
    assert range_intersect(range1, range2)


def test_parallel_tasks_are_queued_based_on_available_ray_worker_cpus():
    start_ts = time.time_ns()

    runlogs = flatten(
        run_tasks(
            [
                PythonTask(lambda _: time.sleep(0.5), task_id="t0"),
                PythonTask(lambda _: time.sleep(0.5), task_id="t1"),
                PythonTask(lambda _: time.sleep(0.5), task_id="t2"),
                PythonTask(lambda _: time.sleep(0.5), task_id="t3"),
            ],
            TaskDependencies(),
        )
    )
    end_ts = time.time_ns()

    # Check 1: with only 2 CPU:s running the above tasks with no constraints should
    # take > 1 secs.
    duration_ms = (end_ts - start_ts) // 1000000
    assert duration_ms >= 1000, duration_ms

    task_runtime_ranges = [
        range(runlog["out.timing.start_ts"], runlog["out.timing.end_ts"])
        for runlog in runlogs
    ]

    # Check 2: if tasks are run on 2 CPU:s the intersection of three runtime ranges
    # should always be empty.
    for r1, r2, r3 in itertools.combinations(task_runtime_ranges, 3):
        assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))
