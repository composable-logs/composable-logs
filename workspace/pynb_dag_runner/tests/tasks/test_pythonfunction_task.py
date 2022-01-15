import time, random, itertools
from typing import List, Set, Tuple

#
import pytest
import opentelemetry as otel

#
from pynb_dag_runner.opentelemetry_helpers import SpanId, Spans
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies
from pynb_dag_runner.helpers import (
    one,
    pairs,
    flatten,
    range_intersect,
    range_intersection,
    range_is_empty,
)
from pynb_dag_runner.core.dag_runner import (
    TaskOutcome,
    fan_in,
    run_in_sequence,
    start_and_await_tasks,
    RemoteTaskP,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    get_duration_range_us,
    read_key,
    get_span_id,
    get_span_exceptions,
    Spans,
    SpanDict,
    SpanRecorder,
)


def assert_compatibility(spans: Spans, task_id_dependencies):
    """
    Test:
     - generic invariances for span timings (Steps 1, 2)
     - order constraints in dependency DAG are satisfied by output timings (Step 3)

    *** TODO: to be rewritten based on Opentelemetry spans ****
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

    # Step 3: Span durations should satisfy the same order constraints as in DAG
    # run order dependencies.
    for rule in task_id_dependencies:
        spans_from = spans.filter(["attributes", "task_id"], rule["from"])
        spans_to = spans.filter(["attributes", "task_id"], rule["to"])

        ts0 = max([get_duration_range_us(s).stop for s in spans_from])
        ts1 = min([get_duration_range_us(s).start for s in spans_to])
        assert ts0 < ts1


### ---- Test Python task evaluation ----


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

        # --- check retry spans ---
        retry_wrapper_span = one(spans.filter(["name"], "retry-wrapper"))
        assert spans.contains_path(top_task_span, retry_wrapper_span)
        assert read_key(retry_wrapper_span, ["attributes", "max_nr_retries"]) == 1

        retry_span = one(spans.filter(["name"], "retry-call"))
        assert spans.contains_path(top_task_span, retry_wrapper_span, retry_span)
        assert read_key(retry_span, ["attributes", "retry_nr"]) == 0

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
            retry_wrapper_span,
            retry_span,
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

        # --- check call-python-function span, this should exist but is not logged ---
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

            def f(arg):
                baggage = otel.baggage.get_all()
                if int(baggage["retry_nr"]) <= 1:
                    time.sleep(1e6)
                else:
                    raise Exception("Failed to run")

            tasks = [
                task_from_python_function(
                    f=f,
                    tags={"function_id": "foo"},
                    max_nr_retries=10,
                    timeout_s=3.0,
                )
            ]

            _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "task-dependency")) == 0

        top_task_span = one(spans.filter(["name"], "execute-task"))
        assert top_task_span["status"] == {
            "description": "Remote function call failed",
            "status_code": "ERROR",
        }

        top_retry_span = one(spans.filter(["name"], "retry-wrapper"))
        assert spans.contains_path(top_task_span, top_retry_span)
        assert read_key(top_retry_span, ["attributes", "max_nr_retries"]) == 10

        retry_call_spans = spans.filter(["name"], "retry-call")
        assert len(retry_call_spans) == 10

        for retry_span in retry_call_spans:
            retry_subspans = spans.restrict_by_top(retry_span)

            timeout_span: SpanDict = one(
                retry_subspans.filter(["name"], "timeout-guard")
            )
            call_python_function_spans = retry_subspans.filter(
                ["name"], "call-python-function"
            )

            if read_key(retry_span, ["attributes", "retry_nr"]) <= 1:
                assert timeout_span["status"] == {
                    "description": "Timeout",
                    "status_code": "ERROR",
                }
                assert len(call_python_function_spans) == 0
            else:
                assert timeout_span["status"] == {"status_code": "OK"}

                assert len(call_python_function_spans) == 1

            assert spans.contains_path(
                top_task_span,
                top_retry_span,
                timeout_span,
                *call_python_function_spans,
            )

        # assert_compatibility(spans)

    validate_spans(get_test_spans())


def test__task_retries__task_is_retried_until_success():
    def get_test_spans():
        with SpanRecorder() as rec:

            def f(arg):
                baggage = otel.baggage.get_all()
                if int(baggage["retry_nr"]) in [0, 1, 2]:
                    time.sleep(1e6)
                elif int(baggage["retry_nr"]) == 3:
                    raise Exception("Unable to run when retry_nr=3")
                else:
                    pass  # success on index >= 4 (5th call)

            tasks = [
                task_from_python_function(
                    f=f,
                    tags={"function_id": "test_task"},
                    max_nr_retries=10,  # 5 retries is needed for success
                    timeout_s=1.5,
                )
            ]

            _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")

        return rec.spans

    def validate_spans(spans: Spans):
        # assert_compatibility(spans)

        # Top task span is success
        top_task_span = one(spans.filter(["name"], "execute-task"))
        assert (
            read_key(top_task_span, ["attributes", "tags.function_id"]) == "test_task"
        )
        assert read_key(top_task_span, ["attributes", "tags.task_type"]) == "Python"
        assert top_task_span["status"] == {"status_code": "OK"}

        retry_call_spans = spans.filter(["name"], "retry-call")
        assert len(retry_call_spans) == 5

        for top_retry_span in retry_call_spans:
            retry_subspans = spans.restrict_by_top(top_retry_span)

            timeout_span: SpanDict = one(
                retry_subspans.filter(["name"], "timeout-guard")
            )
            call_python_function_spans = retry_subspans.filter(
                ["name"], "call-python-function"
            )

            if read_key(top_retry_span, ["attributes", "retry_nr"]) in [0, 1, 2]:
                assert timeout_span["status"] == {
                    "description": "Timeout",
                    "status_code": "ERROR",
                }
                assert len(call_python_function_spans) == 0
            elif read_key(top_retry_span, ["attributes", "retry_nr"]) == 3:
                assert timeout_span["status"] == {"status_code": "OK"}
                assert one(call_python_function_spans)["status"] == {
                    "status_code": "ERROR",
                    "description": "Failure",
                }
            elif read_key(top_retry_span, ["attributes", "retry_nr"]) == 4:
                assert timeout_span["status"] == {"status_code": "OK"}
                assert one(call_python_function_spans)["status"] == {
                    "status_code": "OK"
                }
            else:
                assert False

            assert spans.contains_path(
                top_task_span,
                top_retry_span,
                timeout_span,
                *call_python_function_spans,
            )

    validate_spans(get_test_spans())


### ---- test order dependence for Python tasks ----


@pytest.mark.asyncio
@pytest.mark.parametrize("dummy_loop_parameter", range(1))
@pytest.mark.parametrize(
    "arg",
    [
        #
        # --------------------------- Graphs without fan-in ----------------------------
        #
        # <empty graph, no dependencies>
        #
        {
            "tasks_to_start": [0, 1, 2, 3, 4],
            "tasks_to_await": [0, 1, 2, 3, 4],
            "in_seqs": [],
            "fan_ins": [],
        },
        #
        #  t0  --->  t1
        #
        {
            "tasks_to_start": [0, 2, 3, 4],
            "tasks_to_await": [1, 2, 3, 4],
            "in_seqs": [(0, 1)],
            "fan_ins": [],
        },
        #
        #  t0  --->  t1
        #
        #  t2  --->  t3  --->  t4
        #
        {
            "tasks_to_start": [0, 2],
            "tasks_to_await": [1, 4],
            "in_seqs": [(0, 1), (2, 3, 4)],
            "fan_ins": [],
        },
        {
            "tasks_to_start": [0, 2],
            "tasks_to_await": [1, 4],
            # same as above but with some duplicate in_seq:s
            "in_seqs": [(0, 1), (2, 3, 4), (2, 3), (2, 3, 4)],
            "fan_ins": [],
        },
        #
        #  t0  --->  t1  --->  t2  --->  t3  --->  t4
        #
        {
            "tasks_to_start": [0],
            "tasks_to_await": [4],
            "in_seqs": [(0, 1, 2, 3, 4)],
            "fan_ins": [],
        },
        #
        #      --->  t0  --->  t1
        #     /
        #  t2  --->  t3  --->  t4
        #
        {
            "tasks_to_start": [2],
            "tasks_to_await": [1, 4],
            "in_seqs": [(2, 0, 1), (2, 3, 4)],
            "fan_ins": [],
        },
        #
        # ----------------------------- Graphs with fan-in -----------------------------
        #
        #  t0  --->  t1  ---\
        #                    v
        #  t2  --->  t3  ---> t4
        #
        {
            "tasks_to_start": [0, 2],
            "tasks_to_await": [4],
            "in_seqs": [(0, 1), (2, 3)],
            "fan_ins": [([1, 3], 4)],
        },
        #
        #       --->  t1  ---\
        #      /              v
        #  t0  ---->  t2  --->  t4
        #      \              ^
        #       --->  t3  ---/
        #
        {
            "tasks_to_start": [0],
            "tasks_to_await": [4],
            "in_seqs": [(0, 1), (0, 2), (0, 3)],
            "fan_ins": [([1, 2, 3], 4)],
        },
        #
        #  t0  ---\
        #          v
        #            t1  ---\
        #          ^         v
        #  t2  ---/            t4
        #                    ^
        #  t3  -------------/
        #
        {
            "tasks_to_start": [0, 2, 3],
            "tasks_to_await": [4],
            "in_seqs": [],
            "fan_ins": [([0, 2], 1), ([1, 3], 4)],
        },
        #
        #  t0  ------>  t1  --->  t2
        #      \             \ ^
        #       \             X
        #        \           / v
        #         --->  t3  --->  t4
        #
        {
            "tasks_to_start": [0],
            "tasks_to_await": [2, 4],
            "in_seqs": [(0, 1), (0, 3)],
            "fan_ins": [([1, 3], 2), ([1, 3], 4)],
        },
    ],
)
async def test_random_sleep_tasks_with_order_dependencies(
    dummy_loop_parameter, arg, ray_reinit
):
    """
    This test is memory sensitive. If we do not reinit Ray cluster before every
    test, we may run out of memory (!)
    """
    arg_tasks_to_start: List[int] = arg["tasks_to_start"]
    arg_tasks_to_await: List[int] = arg["tasks_to_await"]
    arg_in_seqs: List[List[int]] = arg["in_seqs"]
    arg_fan_ins: List[Tuple[List[int], int]] = arg["fan_ins"]

    async def get_test_spans():
        with SpanRecorder() as rec:

            def random_sleep(arg):
                time.sleep(random.randint(10, 100) / 1000)

            tasks = [
                task_from_python_function(
                    f=random_sleep,
                    tags={"task_id": f"t{k}"},
                )
                for k in range(5)
            ]

            for in_seq in arg_in_seqs:
                run_in_sequence(*[tasks[k] for k in in_seq])

            for tasks_dep, task_target in arg_fan_ins:
                fan_in([tasks[k] for k in tasks_dep], tasks[task_target])

            time.sleep(0.5)  # test if this changes outcome

            _ = start_and_await_tasks(
                tasks_to_start=[tasks[k] for k in arg_tasks_to_start],
                tasks_to_await=[tasks[k] for k in arg_tasks_to_await],
                timeout_s=100,
                arg="dummy value",
            )

            # assert all tasks have completed
            for task in tasks:
                assert await task.has_completed.remote() == True

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans.filter(["name"], "execute-task")) == 5

        def lookup_task_span_id(task_nr: int) -> SpanId:
            assert task_nr in range(5)
            return get_span_id(
                one(
                    spans.filter(["name"], "execute-task")
                    #
                    .filter(["attributes", "tags.task_id"], f"t{task_nr}")
                )
            )

        expected_dependencies: List[Tuple[SpanId, SpanId]] = [
            (lookup_task_span_id(a), lookup_task_span_id(b))
            for entry in arg_in_seqs
            for a, b in pairs(entry)
        ]

        for target_dep_list, target_task in arg_fan_ins:
            for dependency in target_dep_list:
                expected_dependencies += [
                    (lookup_task_span_id(dependency), lookup_task_span_id(target_task))
                ]

        log_dependencies: Set[Tuple[SpanId, SpanId]] = extract_task_dependencies(spans)

        assert set(expected_dependencies) == log_dependencies

        # assert_compatibility(spans)

    validate_spans(await get_test_spans())
