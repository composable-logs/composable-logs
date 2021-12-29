import time, random
from typing import List, Set, Dict, Tuple, Optional, Any, Callable

#
import pytest, ray

#
from pynb_dag_runner.core.dag_runner import (
    Task,
    RemoteTaskP,
    task_from_python_function,
    TaskOutcome,
    TaskDependencies,
    run_in_sequence,
    in_parallel,
    fan_in,
    run_tasks,
    start_and_await_tasks,
)
from pynb_dag_runner.opentelemetry_helpers import (
    SpanId,
    get_span_id,
    has_keys,
    read_key,
    is_parent_child,
    get_duration_s,
    iso8601_to_epoch_s,
    get_duration_range_us,
    get_span_exceptions,
    Span,
    SpanDict,
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.helpers import A, one

#
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


def test__task_ot__async_wait_for_task():
    def f(_):
        time.sleep(0.125)
        return 43

    task = task_from_python_function(f, tags={"foo": "f"})

    [outcome] = start_and_await_tasks([task], [task], timeout_s=10)

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert outcome.return_value == 43


def dependency_span__to__from_to_ids(dep_span: SpanDict) -> Tuple[SpanId, SpanId]:
    return (
        dep_span["attributes"]["from_task_span_id"],
        dep_span["attributes"]["to_task_span_id"],
    )


def test__task_ot__task_orchestration__run_three_tasks_in_sequence():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:

            def f(_):
                time.sleep(0.125)
                return 43

            def g(arg):
                time.sleep(0.125)
                assert isinstance(arg, TaskOutcome)
                assert arg.error is None
                assert arg.return_value == 43
                return arg.return_value + 1

            def h(arg):
                time.sleep(0.125)
                assert isinstance(arg, TaskOutcome)
                assert arg.error is None
                assert arg.return_value == 44
                return arg.return_value + 1

            tasks: List[RemoteTaskP] = [
                task_from_python_function(f, tags={"foo": "f"}),
                task_from_python_function(g, tags={"foo": "g"}),
                task_from_python_function(h, tags={"foo": "h"}),
            ]
            task_f, task_g, task_h = tasks

            # define task dependencies
            run_in_sequence(task_f, task_g, task_h)

            # no task has has started
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == False
                assert ray.get(task.has_completed.remote()) == False

            [outcome] = start_and_await_tasks([task_f], [task_h], timeout_s=10)

            assert isinstance(outcome, TaskOutcome)
            assert outcome.error is None
            assert outcome.return_value == 45

            # all tasks have completed, and we can query results repeatedly
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == True
                assert ray.get(task.has_completed.remote()) == True
                assert isinstance(ray.get(task.get_task_result.remote()), TaskOutcome)

        return sr.spans

    def validate_spans(spans: Spans):
        deps = spans.filter(["name"], "task-dependency").sort_by_start_time()
        assert len(deps) == 2
        dep_fg, dep_gh = deps

        def lookup_task_span_id(func_name: str) -> SpanId:
            return get_span_id(one(spans.filter(["attributes", "tags.foo"], func_name)))

        # Check that span_id:s referenced in task relationships are found. This may
        # fail if span_id:s are not correctly formatted (eg. with 0x prefix).
        for d in [dep_fg, dep_gh]:
            for k in ["from_task_span_id", "to_task_span_id"]:
                assert spans.contains_span_id(d["attributes"][k])

        # check that dependency relations correspond to "f -> g" and "g -> h"
        assert dependency_span__to__from_to_ids(dep_fg) == (
            lookup_task_span_id(func_name="f"),
            lookup_task_span_id(func_name="g"),
        )

        assert dependency_span__to__from_to_ids(dep_gh) == (
            lookup_task_span_id(func_name="g"),
            lookup_task_span_id(func_name="h"),
        )

    validate_spans(get_test_spans())


def test__task_ot__task_orchestration__fan_in_two_tasks():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:

            def f1(_):
                time.sleep(0.1)
                return 43

            def f2(_):
                time.sleep(0.2)
                return 44

            def f_fan_in(arg):
                # argument should be list of TaskOutcome:s from f1 and f2
                assert isinstance(arg, list)
                assert len(arg) == 2
                for fan_in_outcome in arg:
                    assert isinstance(fan_in_outcome, TaskOutcome)
                    assert fan_in_outcome.error is None
                    assert fan_in_outcome.return_value in [43, 44]

                time.sleep(0.3)
                return 45

            tasks: List[RemoteTaskP] = [
                task_from_python_function(f1, tags={"foo": "f1"}),
                task_from_python_function(f2, tags={"foo": "f2"}),
                task_from_python_function(f_fan_in, tags={"foo": "fan_in"}),
            ]
            task_1, task_2, task_fan_in = tasks

            # define task dependencies
            fan_in([task_1, task_2], task_fan_in)

            # no task has has started
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == False
                assert ray.get(task.has_completed.remote()) == False

            [outcome] = start_and_await_tasks(
                [task_1, task_2], [task_fan_in], timeout_s=10
            )

            assert isinstance(outcome, TaskOutcome)
            assert outcome.error is None
            assert outcome.return_value == 45

            # all tasks have completed, and we can query results repeatedly
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == True
                assert ray.get(task.has_completed.remote()) == True
                assert isinstance(ray.get(task.get_task_result.remote()), TaskOutcome)

        return sr.spans

    def validate_spans(spans: Spans):
        deps = spans.filter(["name"], "task-dependency")
        assert len(deps) == 2
        dep_a, dep_b = deps

        logged_dependencies: Set[Tuple[SpanId, SpanId]] = set(
            [
                dependency_span__to__from_to_ids(dep_a),
                dependency_span__to__from_to_ids(dep_b),
            ]
        )

        def lookup_task_span_id(func_name: str) -> SpanId:
            return get_span_id(one(spans.filter(["attributes", "tags.foo"], func_name)))

        expected_dependencies: Set[Tuple[SpanId, SpanId]] = set(
            [
                (
                    lookup_task_span_id(func_name="f1"),
                    lookup_task_span_id(func_name="fan_in"),
                ),
                (
                    lookup_task_span_id(func_name="f2"),
                    lookup_task_span_id(func_name="fan_in"),
                ),
            ]
        )

        assert expected_dependencies == logged_dependencies

    validate_spans(get_test_spans())


def test__task_ot__task_orchestration__run_three_tasks_in_parallel__failed():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:
            test_exception_msg = "f2-exception"

            def f1(arg: int):
                time.sleep(0.25 * random.random())
                assert arg == 42
                return 1234

            def f2(arg: int):
                time.sleep(0.25 * random.random())
                assert arg == 42
                raise Exception(test_exception_msg)

            def f3(arg: int):
                time.sleep(0.25 * random.random())
                assert arg == 42
                return 123

            tasks: List[RemoteTaskP] = [
                task_from_python_function(f1, tags={"foo": "f1"}),
                task_from_python_function(f2, tags={"foo": "f2"}),
                task_from_python_function(f3, tags={"foo": "f3"}),
            ]

            outcomes = start_and_await_tasks(tasks, tasks, timeout_s=10, arg=42)

            assert all(isinstance(outcome, TaskOutcome) for outcome in outcomes)

            assert [outcome.return_value for outcome in outcomes] == [1234, None, 123]

            assert outcomes[0].error is None
            assert test_exception_msg in str(outcomes[1].error)
            assert outcomes[2].error is None
        return sr.spans

    def validate_spans(spans: Spans):
        # TODO: no dependency information is now logged from parallel tasks
        deps = spans.filter(["name"], "task-dependency").sort_by_start_time()
        assert len(deps) == 0

    validate_spans(get_test_spans())
