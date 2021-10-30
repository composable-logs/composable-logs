import time, random, itertools
from pathlib import Path
from typing import List

#
import pytest, ray

#
from pynb_dag_runner.tasks.tasks import (
    PythonFunctionTask,
    PythonFunctionTask_OT,
    RunParameters,
    get_task_dependencies,
)

#
from pynb_dag_runner.helpers import (
    one,
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
    read_json,
)
from pynb_dag_runner.core.dag_runner import TaskDependencies, run_tasks
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    has_keys,
    Spans,
    SpanRecorder,
    get_duration_range_us,
)


def assert_compatibility(spans: Spans, task_id_dependencies):
    """
    Test:
     - generic invariances for runlog timings (Steps 1, 2)
     - order constraints in dependency DAG are satisfied by output timings (Step 3)
    """

    # Step 1: all task-id:s in order dependencies must have at least one runlog
    # entry. (The converse need not hold.)
    task_ids_in_spans: List[str] = []
    for span in spans:
        if has_keys(span, ["attributes", "task_id"]):
            task_ids_in_spans.append(read_key(span, ["attributes", "task_id"]))

    task_ids_in_dependencies: List[str] = flatten(
        [[d["from"], d["to"]] for d in task_id_dependencies]
    )
    assert set(task_ids_in_dependencies) <= set(task_ids_in_spans)

    # Step 2: A task retry should not start before previous attempt for running task
    # has finished.
    #
    # def get_runlogs(task_id):
    #    return [runlog for runlog in runlog_results if runlog["task_id"] == task_id]
    #
    # check retry timings
    # for task_id in task_ids_in_spans:
    #    task_id_retries = get_runlogs(task_id)
    #    for idx, _ in enumerate(task_id_retries):
    #        if idx > 0:
    #            assert (
    #                task_id_retries[idx - 1]["out.timing.end_ts"]
    #                < task_id_retries[idx]["out.timing.start_ts"]
    #            )

    # Step 3: Runlog timings satisfy the same order constraints as in DAG run order
    # dependencies.
    for rule in task_id_dependencies:
        spans_from = spans.filter(["attributes", "task_id"], rule["from"])
        spans_to = spans.filter(["attributes", "task_id"], rule["to"])

        ts0 = max([get_duration_range_us(s).stop for s in spans_from])
        ts1 = min([get_duration_range_us(s).start for s in spans_to])
        assert ts0 < ts1


### ---- Tests for get_task_dependencies ----


def test_get_task_dependencies():
    assert len(get_task_dependencies(TaskDependencies())) == 0

    t0 = PythonFunctionTask_OT(f=lambda _: None, task_id="t0")
    t1 = PythonFunctionTask_OT(f=lambda _: None, task_id="t1")
    t2 = PythonFunctionTask_OT(f=lambda _: None, task_id="t2")
    t3 = PythonFunctionTask_OT(f=lambda _: None, task_id="t3")

    assert get_task_dependencies((t0 >> t1 >> t2) + TaskDependencies(t1 >> t3)) == [
        {"from": "t0", "to": "t1"},
        {"from": "t1", "to": "t2"},
        {"from": "t1", "to": "t3"},
    ]


### ---- Test PythonFunctionTask evaluation ----


def test_tasks_runlog_output():
    def get_test_spans():
        with SpanRecorder() as rec:
            dependencies = TaskDependencies()
            run_tasks(
                [
                    PythonFunctionTask_OT(lambda _: 123, task_id="my_task_id"),
                ],
                dependencies,
            )
        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        task_span = one(spans.filter(["name"], "python-task"))

        assert read_key(task_span, ["attributes", "task_id"]) == "my_task_id"

        timeout_span = one(spans.filter(["name"], "timeout-guard"))
        call_span = one(spans.filter(["name"], "call-python-function"))
        assert spans.contains_path(task_span, timeout_span, call_span)

        assert task_span["attributes"].keys() == set(
            ["run_id", "task_id", "retry.max_retries", "retry.nr"]
        )

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


def _get_time_range(spans, span_id: str):
    span = one(
        spans.filter(["name"], "python-task")
        # -
        .filter(["attributes", "task_id"], span_id)
    )

    return get_duration_range_us(span)


def test_tasks_run_in_parallel():
    def get_test_spans():
        with SpanRecorder() as rec:
            dependencies = TaskDependencies()

            run_tasks(
                [
                    PythonFunctionTask_OT(lambda _: time.sleep(1.0), task_id="t0"),
                    PythonFunctionTask_OT(lambda _: time.sleep(1.0), task_id="t1"),
                ],
                dependencies,
            )

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        assert len(spans.filter(["name"], "python-task")) == 2

        t0_us_range = _get_time_range(spans, "t0")
        t1_us_range = _get_time_range(spans, "t1")

        # Check: since there are no order constraints, the time ranges should
        # overlap provided tests are run on 2+ CPUs
        assert range_intersect(t0_us_range, t1_us_range)

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


def test_parallel_tasks_are_queued_based_on_available_ray_worker_cpus():
    def get_test_spans():
        with SpanRecorder() as rec:
            start_ts = time.time_ns()

            dependencies = TaskDependencies()
            run_tasks(
                [
                    PythonFunctionTask_OT(lambda _: time.sleep(0.5), task_id="t0"),
                    PythonFunctionTask_OT(lambda _: time.sleep(0.5), task_id="t1"),
                    PythonFunctionTask_OT(lambda _: time.sleep(0.5), task_id="t2"),
                    PythonFunctionTask_OT(lambda _: time.sleep(0.5), task_id="t3"),
                ],
                dependencies,
            )

            end_ts = time.time_ns()

            # Check 1: with only 2 CPU:s running the above tasks with no constraints
            # should take > 1 secs.
            duration_ms = (end_ts - start_ts) // 1000000
            assert duration_ms >= 1000, duration_ms

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        assert len(spans.filter(["name"], "python-task")) == 4

        task_runtime_ranges = [
            _get_time_range(spans, span_id) for span_id in ["t0", "t1", "t2", "t3"]
        ]

        # Check 2: if tasks are run on 2 CPU:s the intersection of three runtime ranges
        # should always be empty.
        for r1, r2, r3 in itertools.combinations(task_runtime_ranges, 3):
            assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


### ---- test order dependence for PythonFunctionTask:s ----


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
        #  t2  --->  t3  --->  t4
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
        #      --->  t0  --->  t1
        #     /
        #  t2  --->  t3  --->  t4
        #
        ["t2 >> t0", "t2 >> t3", "t0 >> t1", "t3 >> t4"],
        #
        #  t0  --->  t1  --->  t2  --->  t3  --->  t4
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
        #  t2  ---/            t4
        #                    ^
        #  t3  -------------/
        #
        ["t0 >> t1", "t2 >> t1", "t1 >> t4", "t3 >> t4"],
        # same as above, and two redundant constraints
        ["t0 >> t1", "t2 >> t1", "t1 >> t4", "t3 >> t4", "t0 >> t4", "t2 >> t4"],
        #
        #  t0  ------>  t1  --->  t2
        #      \             \ ^
        #       \             X
        #        \           / v
        #         --->  t3  --->  t4
        #
        ["t0 >> t1", "t0 >> t3", "t1 >> t2", "t3 >> t4", "t1 >> t4", "t3 >> t2"],
    ],
)
def test_random_sleep_tasks_with_order_dependencies(dependencies_list):
    def get_test_spans():
        with SpanRecorder() as rec:

            def sleep_f():
                sleep_ms = random.randint(10, 100)
                return lambda _: time.sleep(sleep_ms / 1000)

            # local variables t0, .., t4 need to be defined for dependencies
            t0 = PythonFunctionTask_OT(sleep_f(), task_id="t0")
            t1 = PythonFunctionTask_OT(sleep_f(), task_id="t1")
            t2 = PythonFunctionTask_OT(sleep_f(), task_id="t2")
            t3 = PythonFunctionTask_OT(sleep_f(), task_id="t3")
            t4 = PythonFunctionTask_OT(sleep_f(), task_id="t4")

            # See https://stackoverflow.com/questions/55084171
            f_locals = locals()
            dependencies = TaskDependencies(
                *[eval(d, f_locals) for d in dependencies_list]
            )
            run_tasks([t0, t1, t2, t3, t4], dependencies)

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        assert len(spans.filter(["name"], "python-task")) == 5

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


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

    # rewrite after retry-task logs to OpenTelemetry
    # assert_compatibility(runlog_results, get_task_dependencies(dependencies))


def test_task_retries__multiple_retrys_should_run_in_parallel():
    def get_test_spans():
        with SpanRecorder() as rec:

            def sleep_f(runparameters: RunParameters):
                if runparameters["retry_nr"] <= 2:
                    time.sleep(1e6)

            t0 = PythonFunctionTask_OT(
                sleep_f, task_id="t0", timeout_s=1, n_max_retries=10
            )
            t1 = PythonFunctionTask_OT(
                sleep_f, task_id="t1", timeout_s=1, n_max_retries=10
            )

            dependencies = TaskDependencies()
            run_tasks([t0, t1], dependencies)

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        assert_compatibility(spans, task_dependencies)

        # On fast multi-core computers we can check that ray.get takes less than 2x the
        # sleep delay in f. However, on slower VMs with only two cores (and possibly other
        # processes?, like github's defaults runners) there may be so much overhead this
        # is not true. Instead we check that that there is some overlap between run times
        # for the two tasks. This seems like a more stable condition.
        top_span_1, top_span_2 = list(spans.filter(["name"], "invoke-task"))

        assert range_intersect(
            get_duration_range_us(top_span_1), get_duration_range_us(top_span_2)
        )

    validate_spans(*get_test_spans())


@pytest.mark.parametrize("dummy_loop_parameter", range(1))
def skip_test_task_retries__retry_and_timeout_composition(dummy_loop_parameter):
    """
    Test composition of both retry and timeout wrappers
    """

    def f(retry_count):
        if retry_count < 5:
            time.sleep(1e6)  # hang computation

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=1,
        success_handler=lambda _: "SUCCESS",
        error_handler=lambda e: f"FAIL:{e}",
    )

    f_retry_timeout = retry_wrapper(
        lambda retry_count: f_timeout(ray.put(retry_count)),
        10,
        is_success=lambda result: result == "SUCCESS",
    )

    results = flatten(ray.get([f_retry_timeout]))

    assert len(results) == 6
    for result in results[:-1]:
        assert result.startswith("FAIL:Timeout error:")
    assert results[-1] == "SUCCESS"
