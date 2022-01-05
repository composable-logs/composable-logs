import time, random, itertools
from typing import List

#
import pytest

#
from pynb_dag_runner.tasks.tasks import (
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
from pynb_dag_runner.core.dag_runner import (
    TaskDependencies,
    TaskOutcome,
    run_tasks,
    start_and_await_tasks,
    RemoteTaskP,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    get_duration_range_us,
    read_key,
    get_span_exceptions,
    Spans,
    SpanDict,
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


### ---- Test PythonFunctionTask evaluation ----


@pytest.mark.parametrize("task_should_fail", [True, False])
def test__python_function_task__outputs_otel_logs__should_fail_as_parameter(
    task_should_fail,
):
    ERROR_MSG = "!!!Exception-12342!!!"

    def get_test_spans():
        with SpanRecorder() as rec:

            def f(_):
                if task_should_fail:
                    raise Exception(ERROR_MSG)
                else:
                    return 123

            task: RemoteTaskP = task_from_python_function(
                f, tags={"foo": "my_test_func"}
            )
            [outcome] = start_and_await_tasks(
                [task], [task], timeout_s=10, arg="dummy value"
            )

            # check Task outcome
            assert isinstance(outcome, TaskOutcome)
            if task_should_fail:
                assert ERROR_MSG in str(outcome.error)
            else:
                assert outcome.return_value == 123

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "task-dependency")) == 0

        top_task_span: SpanDict = one(spans.filter(["name"], "execute-task"))
        assert read_key(top_task_span, ["attributes", "tags.foo"]) == "my_test_func"
        assert read_key(top_task_span, ["attributes", "tags.task_type"]) == "Python"

        error_spans: Spans = Spans(
            [span for span in spans if len(get_span_exceptions(span)) > 0]
        )
        if task_should_fail:
            assert len(error_spans) > 0

            assert top_task_span["status"] == {
                "status_code": "ERROR",
                "description": "Remote function call failed",
            }
        else:
            assert len(error_spans) == 0
            assert top_task_span["status"] == {"status_code": "OK"}

        # --- check retry-wrapper span (TODO) ---
        # retries_span = one(spans.filter(["name"], "retry-wrapper"))
        # run_span = one(spans.filter(["name"], "task-run"))
        # assert read_key(run_span, ["attributes", "retry.max_retries"]) == 1
        # assert read_key(run_span, ["attributes", "retry.nr"]) == 0
        # assert run_span["attributes"].keys() == set(
        #     ["task_id", "retry.max_retries", "retry.nr"]
        # )

        # --- check timeout-guard span ---
        timeout_span: SpanDict = one(spans.filter(["name"], "timeout-guard"))
        assert timeout_span["status"] == {"status_code": "OK"}  # no timeouts

        # --- check call-python-function span ---
        call_function_span: SpanDict = one(
            spans.filter(["name"], "call-python-function")
        )

        if task_should_fail:
            assert call_function_span["status"] == {
                "status_code": "ERROR",
                "description": "Failure",
            }

            # call span should record exception from function
            call_function_span_exception = one(get_span_exceptions(call_function_span))[
                "attributes"
            ]
            assert call_function_span_exception["exception.type"] == "Exception"
            assert call_function_span_exception["exception.message"] == ERROR_MSG
        else:
            assert call_function_span["status"] == {"status_code": "OK"}

        # check nesting of above spans
        assert spans.contains_path(
            top_task_span,
            # retries_span,
            # run_span,
            timeout_span,
            call_function_span,
        )

        # assert_compatibility(spans)

    validate_spans(get_test_spans())


def test__python_function_task__otel_logs_for_stuck_task():
    def get_test_spans():
        with SpanRecorder() as rec:

            def f(_):
                time.sleep(1e6)

            task: RemoteTaskP = task_from_python_function(
                f, tags={"id": "stuck_function"}, timeout_s=1.0
            )
            [outcome] = start_and_await_tasks(
                [task], [task], timeout_s=10, arg="dummy value"
            )

            # check Task outcome
            assert isinstance(outcome, TaskOutcome)
            assert "timeout" in str(outcome.error)

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "task-dependency")) == 0

        top_task_span: SpanDict = one(spans.filter(["name"], "execute-task"))
        assert read_key(top_task_span, ["attributes", "tags.id"]) == "stuck_function"
        assert read_key(top_task_span, ["attributes", "tags.task_type"]) == "Python"

        # --- check timeout-guard span ---
        timeout_span: SpanDict = one(spans.filter(["name"], "timeout-guard"))
        assert read_key(timeout_span, ["attributes", "timeout_s"]) == 1.0

        assert timeout_span["status"] == {
            "description": "Timeout",
            "status_code": "ERROR",
        }

        # --- check call-python-function span ---
        assert len(spans.filter(["name"], "call-python-function")) == 0

        # check nesting of above spans
        assert spans.contains_path(
            top_task_span,
            timeout_span,
        )

        # assert_compatibility(spans)

    validate_spans(get_test_spans())


def _get_time_range(spans: Spans, function_id: str, inner: bool):
    task_top_span = one(
        spans.filter(["name"], "execute-task")
        # -
        .filter(["attributes", "tags.function_id"], function_id)
    )

    task_spans = spans.restrict_by_top(task_top_span)

    inner_flag_to_span_dict = {
        # inner=True: return time range for span used for (inner) python
        # function call; this is where task cpu resources are reserved.
        True: one(task_spans.filter(["name"], "call-python-function")),
        # inner=False: return time range for top span of entire task
        False: task_top_span,
    }

    return get_duration_range_us(inner_flag_to_span_dict[inner])


def test_tasks_run_in_parallel():
    def get_test_spans():
        with SpanRecorder() as rec:
            tasks = [
                task_from_python_function(
                    lambda _: time.sleep(1.0),
                    tags={"function_id": f"id#{function_id}"},
                    timeout_s=10.0,
                )
                for function_id in range(2)
            ]

            _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "execute-task")) == 2

        t0_us_range = _get_time_range(spans, "id#0", inner=False)
        t1_us_range = _get_time_range(spans, "id#1", inner=False)

        # Check: since there are no order constraints, the time ranges should
        # overlap provided tests are run on 2+ CPUs
        assert range_intersect(t0_us_range, t1_us_range)

        # assert_compatibility(spans)

    validate_spans(get_test_spans())


def test_parallel_tasks_are_queued_based_on_available_ray_worker_cpus():
    def get_test_spans():
        with SpanRecorder() as rec:
            tasks = [
                task_from_python_function(
                    lambda _: time.sleep(0.5),
                    tags={"function_id": f"id#{function_id}"},
                    timeout_s=10.0,
                )
                for function_id in range(4)
            ]

            start_ts = time.time_ns()
            _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")
            end_ts = time.time_ns()

            # Check 1: with only 2 CPU:s (reserved for unit tests, see ray.init call)
            # running the above tasks with no constraints should take > 1 secs.
            duration_ms = (end_ts - start_ts) // 1000000
            assert duration_ms >= 1000, duration_ms
        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "execute-task")) == 4

        task_runtime_ranges = [
            _get_time_range(spans, span_id, inner=True)
            for span_id in [f"id#{function_id}" for function_id in range(4)]
        ]

        # Check 2: since only 2 CPU:s are reserved (for unit tests, see above)
        # the intersection of three runtime ranges should always be empty.
        for r1, r2, r3 in itertools.combinations(task_runtime_ranges, 3):
            assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))

        # assert_compatibility(spans)

    validate_spans(get_test_spans())


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
