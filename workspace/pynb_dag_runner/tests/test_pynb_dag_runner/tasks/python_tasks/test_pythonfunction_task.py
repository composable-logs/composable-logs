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

from .py_test_helpers import get_time_range


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
async def test__python_function_task__random_sleep_tasks_with_order_dependencies(
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
                    attributes={"task.task_id": f"t{k}"},
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
                    .filter(["attributes", "task.task_id"], f"t{task_nr}")
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
