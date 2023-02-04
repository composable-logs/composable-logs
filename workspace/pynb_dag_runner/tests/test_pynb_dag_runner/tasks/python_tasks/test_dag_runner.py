from typing import Any, Dict

# -
import pytest
import opentelemetry as otel

# -
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
    otel_add_baggage,
)
from pynb_dag_runner.opentelemetry_task_span_parser import (
    parse_spans,
)
from pynb_dag_runner.helpers import Success
from pynb_dag_runner.wrappers import task, run_dag, TaskContext

# --- Check that we can propagate baggage into tasks ---
# - note that baggage is converted into strings


def test_opentelemetry_baggage_convert_into_strings():
    @task(task_id="task-f")
    def f():
        return otel.baggage.get_all()

    dag = f()

    workflow_parameters_in = {"workflow.env": "x123"}
    workflow_parameters_out = {**workflow_parameters_in, "workflow.k": "123"}

    with SpanRecorder() as rec:
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("span-to-isolate-baggage-from-other-tests"):
            otel_add_baggage("workflow.k", 123)
            assert run_dag(dag, workflow_parameters=workflow_parameters_in) == Success(
                workflow_parameters_out
            )

    assert len(rec.spans.exception_events()) == 0
    workflow_summary = parse_spans(rec.spans)

    # baggage does not modify paramters
    assert workflow_summary.attributes == workflow_parameters_out


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
    workflow_summary = parse_spans(cl__can_compose_spans)

    assert workflow_summary.attributes == {"workflow.env": "xyz"}

    assert len(workflow_summary.task_runs) == 3

    span_id_to_task_id: Dict[str, str] = {}

    # check logged workflow and task attributes
    for task_summary in workflow_summary.task_runs:  # type: ignore
        assert task_summary.is_success()

        # assert that task has expected attributes logged
        task_id: str = task_summary.attributes["task.id"]  # type: ignore
        assert task_id in TEST_TASK_ATTRIBUTES

        assert task_summary.attributes == {
            "task.id": task_id,
            "task.type": "python",
            **TEST_TASK_ATTRIBUTES[task_id],
            "workflow.env": "xyz",
            "task.num_cpus": 1,
            "task.timeout_s": -1,
        }

        span_id_to_task_id[task_summary.span_id] = task_id

    # check logged task dependencies
    assert len(workflow_summary.task_dependencies) == 2
    for span_id_from, span_id_to in workflow_summary.task_dependencies:
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

    @task(task_id="test_function", task_parameters=task_parameters, timeout_s=12.78)
    def f(C: TaskContext):
        return C.parameters

    with SpanRecorder() as rec:
        assert run_dag(dag=f(), workflow_parameters=workflow_parameters) == Success(
            {
                "task.id": "test_function",
                "task.type": "python",
                "task.num_cpus": 1,
                "task.timeout_s": 12.78,
                **task_parameters,
                **workflow_parameters,
            }
        )

    assert len(rec.spans.exception_events()) == 0
