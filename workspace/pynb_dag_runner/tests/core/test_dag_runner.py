import time, random
from typing import List, Set, Dict, Tuple

#
import ray

#
from pynb_dag_runner.core.dag_runner import (
    RemoteTaskP,
    TaskOutcome,
    task_from_python_function,
    run_in_sequence,
    fan_in,
    start_and_await_tasks,
)
from pynb_dag_runner.opentelemetry_helpers import (
    SpanId,
    get_span_id,
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies
from pynb_dag_runner.helpers import A, one

import opentelemetry as otel


def test__task__can_access_otel_baggage_and_returns_outcome():
    def f(_):
        # check access to OpenTelemetry baggage
        assert otel.baggage.get_all() == {
            "timeout_s": "12.3",
            "num_cpus": 1,
            "run.retry_nr": "0",
            "task.max_nr_retries": "1",
        }

        return 42

    task = task_from_python_function(f, tags={"foo": "f"}, timeout_s=12.3)

    [outcome] = start_and_await_tasks([task], [task], timeout_s=100)

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert outcome.return_value == 42


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

            [outcome] = start_and_await_tasks([task_f], [task_h], timeout_s=100)

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
        def lookup_task_span_id(func_name: str) -> SpanId:
            return get_span_id(one(spans.filter(["attributes", "tags.foo"], func_name)))

        log_dependencies: Set[Tuple[SpanId, SpanId]] = extract_task_dependencies(spans)

        # Check that span_id:s referenced in task relationships are found. This may
        # fail if logged span_id:s are not correctly formatted (eg. with 0x prefix).
        for span_ids in log_dependencies:
            for span_id in span_ids:
                assert spans.contains_span_id(span_id)

        # check that logged dependency relations correspond to "f -> g" and "g -> h"
        assert log_dependencies == set(
            [
                (
                    lookup_task_span_id(func_name="f"),
                    lookup_task_span_id(func_name="g"),
                ),
                (
                    lookup_task_span_id(func_name="g"),
                    lookup_task_span_id(func_name="h"),
                ),
            ]
        )

    validate_spans(get_test_spans())


def test__task_ot__task_orchestration__fan_in_two_tasks():
    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:

            def f1(_):
                time.sleep(0.1)
                return 143

            def f2(_):
                time.sleep(0.2)
                return 144

            def f_fan_in(arg):
                # argument should be list of TaskOutcome:s from f1 and f2
                assert isinstance(arg, list)
                assert len(arg) == 2
                for fan_in_outcome in arg:
                    assert isinstance(fan_in_outcome, TaskOutcome)
                    assert fan_in_outcome.error is None
                    assert fan_in_outcome.return_value in [143, 144]

                time.sleep(0.3)
                return 145

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
                [task_1, task_2], [task_fan_in], timeout_s=100
            )

            assert isinstance(outcome, TaskOutcome)
            assert outcome.error is None
            assert outcome.return_value == 145

            # all tasks have completed, and we can query results repeatedly
            for task in 10 * tasks:
                assert ray.get(task.has_started.remote()) == True
                assert ray.get(task.has_completed.remote()) == True
                assert isinstance(ray.get(task.get_task_result.remote()), TaskOutcome)

        return sr.spans

    def validate_spans(spans: Spans):
        log_dependencies: Set[Tuple[SpanId, SpanId]] = extract_task_dependencies(spans)

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

        assert expected_dependencies == log_dependencies

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

            outcomes = start_and_await_tasks(tasks, tasks, timeout_s=100, arg=42)

            assert all(isinstance(outcome, TaskOutcome) for outcome in outcomes)

            assert [outcome.return_value for outcome in outcomes] == [1234, None, 123]

            assert outcomes[0].error is None
            assert test_exception_msg in str(outcomes[1].error)
            assert outcomes[2].error is None
        return sr.spans

    def validate_spans(spans: Spans):
        assert len(extract_task_dependencies(spans)) == 0

    validate_spans(get_test_spans())
