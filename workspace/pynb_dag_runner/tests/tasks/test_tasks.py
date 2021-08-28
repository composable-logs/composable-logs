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
    Edges,
    TaskDependencies,
    run_tasks,
)
from pynb_dag_runner.wrappers.runlog import Runlog

## TODO: all the below tests should run multiple times for stress testing


def test_tasks_runlog_output():
    runlog_results = flatten(
        run_tasks(
            [
                PythonTask(lambda _: 123, task_id="t1"),
            ],
            TaskDependencies(),
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
            "out.timing.start_ts",
            "out.timing.end_ts",
            "out.timing.duration_ms",
            "out.status",
            "out.error",
            "out.result",
        ]
    )


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


def test_retry_logic_in_python_function_task():
    timeout_s = 5.0

    def f(runlog):
        if runlog["parameters.run.retry_nr"] == 0:
            time.sleep(1e6)  # hang execution to generate a timeout error
        elif runlog["parameters.run.retry_nr"] == 1:
            raise Exception("BOOM!")
        elif runlog["parameters.run.retry_nr"] == 2:
            return 123

    t0 = PythonTask(f, task_id="t0", timeout_s=timeout_s, n_max_retries=3)
    t1 = PythonTask(lambda _: 42, task_id="t1")

    result_runlog = flatten(run_tasks([t0, t1], Edges(t0 >> t1)))

    # All retries should have a distinct run-id:s
    assert len(set([r["parameters.run.id"] for r in result_runlog])) == 4

    # t0 should be run 3 times, t1 once
    assert [r["task_id"] for r in result_runlog] == ["t0", "t0", "t0", "t1"]

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
        for r in result_runlog
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
