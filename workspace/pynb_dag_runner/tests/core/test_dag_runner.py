import time, random
from typing import List, Set, Dict, Tuple, Optional, Any, Callable

#
import pytest, ray

#
from pynb_dag_runner.core.dag_runner import (
    Task,
    RemoteTaskP,
    task_from_func,
    task_from_remote_f,
    TaskOutcome,
    TaskDependencies,
    run_in_sequence,
    in_parallel,
    run_tasks,
    run_and_await_tasks,
)
from pynb_dag_runner.opentelemetry_helpers import (
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
from pynb_dag_runner.helpers import one

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
    def f(*args):
        time.sleep(0.125)
        return 43

    task = task_from_func(f, tags={"foo": "f"})

    outcome = run_and_await_tasks([task], task, timeout_s=10)

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert outcome.return_value == 43


def test__task_ot__task_orchestration__run_three_tasks_in_sequence():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:

            def f(*args):
                if len(args) > 0:
                    raise Exception("f got a paraterer")
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
                task_from_func(f, tags={"foo": "f"}),
                task_from_func(g, tags={"foo": "g"}),
                task_from_func(h, tags={"foo": "h"}),
            ]
            task_f, task_g, task_h = tasks

            # define task dependencies
            run_in_sequence(task_f, task_g, task_h)

            # no has has started
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == False
                assert ray.get(task.has_completed.remote()) == False

            outcome = run_and_await_tasks([task_f], task_h, timeout_s=10)

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

        def get_span_for_task(func_name: str) -> Span:
            assert func_name in ["f", "g", "h"]
            return one(spans.filter(["attributes", "tags.foo"], func_name))

        # Check that span_id:s referenced in task relationships are found. This may
        # fail if span_id:s are not correctly formatted (eg. with 0x prefix).
        for d in [dep_fg, dep_gh]:
            for k in ["from_task_span_id", "to_task_span_id"]:
                assert spans.contains_span_id(d["attributes"][k])

        def dep_from_span(s: SpanDict) -> SpanDict:
            return spans.get_by_span_id(s["attributes"]["from_task_span_id"])

        def dep_to_span(s: SpanDict) -> SpanDict:
            return spans.get_by_span_id(s["attributes"]["to_task_span_id"])

        # check that dependency relations correspond to "f -> g" and "g -> h"
        assert dep_from_span(dep_fg) == get_span_for_task("f")
        assert dep_to_span(dep_fg) == get_span_for_task("g")

        assert dep_from_span(dep_gh) == get_span_for_task("g")
        assert dep_to_span(dep_gh) == get_span_for_task("h")

    validate_spans(get_test_spans())


def test__task_ot__task_orchestration__run_three_tasks_in_parallel__failed():
    def f(*args):
        return 1234

    def g(*args):
        raise Exception("Exception from g")

    def h(*args):
        return 123

    combined_task = in_parallel(*[task_from_func(_f) for _f in [f, g, h]])
    combined_task.start.remote()
    outcome = ray.get(combined_task.get_task_result.remote())

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is not None
    assert [o.return_value for o in outcome.return_value] == [1234, None, 123]


def test__task_ot__task_orchestration__run_three_tasks_in_parallel__success():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:

            def f(*args):
                return 1234

            def g(*args):
                return 123

            def h(*args):
                return 12

            tasks: List[RemoteTaskP] = [
                task_from_func(f, tags={"foo": "f"}),
                task_from_func(g, tags={"foo": "g"}),
                task_from_func(h, tags={"foo": "h"}),
            ]
            all_tasks = in_parallel(*tasks)  # type: ignore
            all_tasks.start.remote()

            outcome = ray.get(all_tasks.get_task_result.remote())

            assert isinstance(outcome, TaskOutcome)
            assert outcome.error is None
            assert [o.return_value for o in outcome.return_value] == [1234, 123, 12]  # type: ignore
        return sr.spans

    def validate_spans(spans: Spans):
        # TODO: no dependency information is now logged from parallel tasks
        deps = spans.filter(["name"], "task-dependency").sort_by_start_time()
        assert len(deps) == 0

    validate_spans(get_test_spans())
