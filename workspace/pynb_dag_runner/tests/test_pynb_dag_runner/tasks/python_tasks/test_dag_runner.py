import time, random
from typing import Any, List, Set, Dict, Tuple

#
import pytest
import ray
import opentelemetry as otel

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
from pynb_dag_runner.opentelemetry_task_span_parser import (
    parse_spans,
    extract_task_dependencies,
)
from pynb_dag_runner.helpers import A, del_key, one, Try, Success, Failure
from pynb_dag_runner.wrappers import task, run_dag, TaskContext


@pytest.mark.skipif(True, reason="remove after move to new Ray interface")
def test__task__can_access_otel_baggage_and_returns_outcome():
    def f(_):
        # check access to OpenTelemetry baggage
        assert otel.baggage.get_all() == {
            "task.timeout_s": "12.3",
            "task.num_cpus": 1,
        }

        return 42

    task = task_from_python_function(f, attributes={"task.foo": "f"}, timeout_s=12.3)

    [outcome] = start_and_await_tasks([task], [task], timeout_s=100)

    assert isinstance(outcome, TaskOutcome)
    assert outcome.error is None
    assert outcome.return_value == 42


# --- Check the following DAG ---
#
#     numeric_input1 --\
#                       v
#                         process
#                       ^
#     numeric_input2 --/
#
TEST_TASK_ATTRIBUTES: Dict[str, Dict[str, Any]] = {
    "input_1": {"task.foo": 12, "task.abc": "abc", "task.gamma": 1.23},
    "input_2": {"task.foo": 23},
    "process": {},
}


@pytest.fixture(scope="module")
def cl__can_compose_spans() -> Spans:
    @task(task_id="input_1", task_parameters=TEST_TASK_ATTRIBUTES["input_1"])
    def input_1():
        return 10

    @task(task_id="input_2", task_parameters=TEST_TASK_ATTRIBUTES["input_2"])
    def input_2(a_variable, C: TaskContext):
        return a_variable + 20

    @task(task_id="process", task_parameters=TEST_TASK_ATTRIBUTES["process"])
    def process(x, y, **kwargs):
        return {"result": x + y, **kwargs}

    dag = process(input_1(), input_2(a_variable=123), test=10)

    with SpanRecorder() as rec:
        assert run_dag(dag, workflow_parameters={"workflow.env": "xyz"}) == Success(
            {
                "result": 10 + (123 + 20),
                "test": 10,
            }
        )

    assert len(rec.spans.exception_events()) == 0

    return rec.spans


def test__cl__can_compose(cl__can_compose_spans: Spans):
    pipeline_summary = parse_spans(cl__can_compose_spans)

    assert pipeline_summary.attributes == {"workflow.env": "xyz"}

    assert len(pipeline_summary.task_runs) == 3

    span_id_to_task_id: Dict[str, str] = {}

    # check logged workflow and task attributes
    for task_summary in pipeline_summary.task_runs:  # type: ignore
        assert task_summary.is_success()

        # assert that task has expected attributes logged
        task_id: str = task_summary.attributes["task.task_id"]  # type: ignore
        assert task_id in TEST_TASK_ATTRIBUTES

        assert task_summary.attributes == {
            "task.task_id": task_id,
            **TEST_TASK_ATTRIBUTES[task_id],
            "workflow.env": "xyz",
            "task.num_cpus": 1,
            "task.timeout_s": -1,
        }

        span_id_to_task_id[task_summary.span_id] = task_id

    # check logged task dependencies
    assert len(pipeline_summary.task_dependencies) == 2
    for span_id_from, span_id_to in pipeline_summary.task_dependencies:
        task_id_from = span_id_to_task_id[span_id_from]
        task_id_to = span_id_to_task_id[span_id_to]

        assert (task_id_from, task_id_to) in [
            ("input_1", "process"),
            ("input_2", "process"),
        ]


# ---


def test__cl__function_parameters_contain_task_and_system_and_global_parameters():
    workflow_parameters = {"workflow.env": "local-test"}
    task_parameters = {"task.X": 123, "task.Y": "indigo"}

    @task(task_id="test_function", task_parameters=task_parameters, timeout_s=1.23)
    def f(C: TaskContext):
        return C.parameters

    with SpanRecorder() as rec:
        assert run_dag(dag=f(), workflow_parameters=workflow_parameters) == Success(
            {
                "task.task_id": "test_function",
                "task.num_cpus": 1,
                "task.timeout_s": 1.23,
                **task_parameters,
                **workflow_parameters,
            }
        )

    assert len(rec.spans.exception_events()) == 0


@pytest.mark.skipif(True, reason="remove after move to new Ray interface")
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
                task_from_python_function(f, attributes={"task.foo": "f"}),
                task_from_python_function(g, attributes={"task.foo": "g"}),
                task_from_python_function(h, attributes={"task.foo": "h"}),
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
            return get_span_id(one(spans.filter(["attributes", "task.foo"], func_name)))

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


@pytest.mark.skipif(True, reason="remove after move to new Ray interface")
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
                task_from_python_function(f1, attributes={"task.foo": "f1"}),
                task_from_python_function(f2, attributes={"task.foo": "f2"}),
                task_from_python_function(f_fan_in, attributes={"task.foo": "fan_in"}),
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
            return get_span_id(one(spans.filter(["attributes", "task.foo"], func_name)))

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


@pytest.mark.skipif(True, reason="remove after move to new Ray interface")
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
                task_from_python_function(f1, attributes={"foo": "f1"}),
                task_from_python_function(f2, attributes={"foo": "f2"}),
                task_from_python_function(f3, attributes={"foo": "f3"}),
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
