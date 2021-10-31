import time, random, itertools
from typing import List

#
import pytest

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
    pairs,
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
)
from pynb_dag_runner.core.dag_runner import TaskDependencies, run_tasks
from pynb_dag_runner.opentelemetry_helpers import (
    get_duration_range_us,
    read_key,
    Spans,
    SpanRecorder,
)


def assert_compatibility(spans: Spans, task_id_dependencies):
    """
    Test:
     - generic invariances for runlog timings (Steps 1, 2)
     - order constraints in dependency DAG are satisfied by output timings (Step 3)
    """

    # Step 1: all task-id:s in order dependencies must have at least one runlog
    # entry. (The converse need not hold.)
    top_spans: Spans = spans.filter(["name"], "invoke-task")
    task_ids_in_spans: List[str] = [span["attributes"]["task_id"] for span in top_spans]
    # each top span should have unique span_id
    assert len(set(task_ids_in_spans)) == len(task_ids_in_spans)

    task_ids_in_dependencies: List[str] = flatten(
        [[d["from"], d["to"]] for d in task_id_dependencies]
    )
    assert set(task_ids_in_dependencies) <= set(task_ids_in_spans)

    # Step 2: A task retry should not start before previous attempt for running task
    # has finished.
    for top_span in top_spans:
        task_id = top_span["attributes"]["task_id"]
        run_spans = list(
            spans.restrict_by_top(top_span)
            .filter(["name"], "task-run")
            .sort_by_start_time()
        )
        assert len(run_spans) >= 1

        for retry_nr, run_span in enumerate(run_spans):
            assert run_span["attributes"]["task_id"] == task_id
            assert run_span["attributes"]["retry.nr"] == retry_nr

            if run_span["attributes"]["retry.max_retries"] > len(run_spans):
                assert list(run_spans)[-1]["status"] == {"status_code": "OK"}

        for s1, s2 in pairs(run_spans):
            assert get_duration_range_us(s1).stop < get_duration_range_us(s2).start

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
        task_span = one(spans.filter(["name"], "invoke-task"))
        assert read_key(task_span, ["attributes", "task_id"]) == "my_task_id"
        assert read_key(task_span, ["attributes", "task_type"]) == "python"

        retries_span = one(spans.filter(["name"], "retry-wrapper"))

        run_span = one(spans.filter(["name"], "task-run"))
        assert read_key(run_span, ["attributes", "task_id"]) == "my_task_id"
        assert read_key(run_span, ["attributes", "retry.max_retries"]) == 1
        assert read_key(run_span, ["attributes", "retry.nr"]) == 0
        assert run_span["attributes"].keys() == set(
            ["run_id", "task_id", "retry.max_retries", "retry.nr"]
        )

        timeout_span = one(spans.filter(["name"], "timeout-guard"))
        call_span = one(spans.filter(["name"], "call-python-function"))

        # check nesting of above spans
        assert spans.contains_path(
            task_span, retries_span, run_span, timeout_span, call_span
        )

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


def _get_time_range(spans: Spans, span_id: str):
    task_top_span = one(
        spans.filter(["name"], "invoke-task")
        # -
        .filter(["attributes", "task_id"], span_id)
    )

    return get_duration_range_us(task_top_span)


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
        assert len(spans.filter(["name"], "invoke-task")) == 2

        t0_us_range = _get_time_range(spans, "t0")
        t1_us_range = _get_time_range(spans, "t1")

        # Check: since there are no order constraints, the time ranges should
        # overlap provided tests are run on 2+ CPUs
        assert range_intersect(t0_us_range, t1_us_range)

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


def test_always_failing_task():
    def get_test_spans():
        with SpanRecorder() as rec:
            dependencies = TaskDependencies()

            def fail_f(runparameters: RunParameters):
                if runparameters["retry.nr"] <= 2:
                    time.sleep(1e6)
                else:
                    raise Exception("Failed to run")

            t0 = PythonFunctionTask_OT(
                fail_f, task_id="always_failing_task", timeout_s=5, n_max_retries=10
            )

            run_tasks([t0], dependencies)

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        top_span = one(spans.filter(["name"], "invoke-task"))
        assert top_span["status"] == {
            "description": "Task failed",
            "status_code": "ERROR",
        }

        run_spans = spans.filter(["name"], "task-run")
        assert len(run_spans) == 10

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
        assert len(spans.filter(["name"], "invoke-task")) == 4

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
        assert len(spans.filter(["name"], "invoke-task")) == 5

        assert_compatibility(spans, task_dependencies)

    validate_spans(*get_test_spans())


def test__task_retries__task_is_retried_until_success():
    def get_test_spans():
        with SpanRecorder() as rec:

            def sleep_f(runparameters: RunParameters):
                if runparameters["retry.nr"] <= 2:
                    raise Exception("Failed to run")
                if runparameters["retry.nr"] in [3]:
                    time.sleep(1e6)
                return True

            t0 = PythonFunctionTask_OT(
                sleep_f, task_id="test_task", timeout_s=5, n_max_retries=10
            )

            dependencies = TaskDependencies()
            run_tasks([t0], dependencies)

        return rec.spans, get_task_dependencies(dependencies)

    def validate_spans(spans: Spans, task_dependencies):
        assert_compatibility(spans, task_dependencies)

        # Top task span is success
        top_span = one(spans.filter(["name"], "invoke-task"))
        assert top_span["attributes"]["task_id"] == "test_task"
        assert top_span["status"] == {"status_code": "OK"}

        spans_under_top: Spans = spans.restrict_by_top(top_span).sort_by_start_time()

        # Check statuses of timeout spans
        statuses = [
            span["status"]
            for span in spans_under_top.filter(
                ["name"], "timeout-guard"
            ).sort_by_start_time()
        ]

        assert len(statuses) == 5
        assert statuses == (
            3 * [{"status_code": "OK"}]
            + [{"status_code": "ERROR", "description": "Timeout"}]
            + [{"status_code": "OK"}]
        )

    validate_spans(*get_test_spans())


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
