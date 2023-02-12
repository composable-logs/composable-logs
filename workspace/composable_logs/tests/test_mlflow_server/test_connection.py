import random, time, requests

# -
import pytest


# -
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.opentelemetry_task_span_parser import parse_spans
from composable_logs.wrappers import task, run_dag
from composable_logs.mlflow_server.server import (
    MLFLOW_HOST,
    MLFLOW_PORT,
    configure_mlflow_connection_variables,
    is_running,
    ensure_running,
    shutdown,
)


@pytest.fixture(scope="module", autouse=True)
def mlflow_server():
    ensure_running()
    yield
    shutdown()


def test_mlflow_server_is_running_without_fixture():
    assert is_running()


def test_mlflow_server_is_running_with_fixture(mlflow_server):
    assert is_running()

    assert requests.get(f"http://{MLFLOW_HOST}:{MLFLOW_PORT}/status").json() == {
        "status": "OK"
    }


def get_test_spans():
    # Run two tasks in parallel and check that ML Flow data is not mixed up between
    # tasks.

    @task(task_id="ml_flow_tester_1")
    def ml_flow_tester_1():
        configure_mlflow_connection_variables()

        import mlflow

        for k in range(10):
            time.sleep(random.random() * 0.12)
            mlflow.log_param(f"1-logged-key-{k}", "1-value-{k}")

    @task(task_id="ml_flow_tester_2")
    def ml_flow_tester_2():
        configure_mlflow_connection_variables()

        import mlflow

        for k in range(10):
            time.sleep(random.random() * 0.08)
            mlflow.log_param(f"2-logged-key-{k}", "2-value-{k}")

    with SpanRecorder() as rec:
        run_dag([ml_flow_tester_1(), ml_flow_tester_2()])

    return rec.spans


def test_mlflow_data_from_parallel_tasks_are_split_correctly(mlflow_server):
    spans: Spans = get_test_spans()

    workflow_summary = parse_spans(spans)
    assert workflow_summary.is_success()

    for task_summary in workflow_summary.task_runs:  # type: ignore
        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 10

        assert task_summary.task_id in ["ml_flow_tester_1", "ml_flow_tester_2"]
        if task_summary.task_id == "ml_flow_tester_1":
            assert task_summary.logged_values.keys() == {
                f"1-logged-key-{k}" for k in range(10)
            }
        if task_summary.task_id == "ml_flow_tester_2":
            assert task_summary.logged_values.keys() == {
                f"2-logged-key-{k}" for k in range(10)
            }


# def _test_mlflow_logging_different_types():
#     spans = []

#     for s in spans:
#         print(100 * "-")
#         import json

#         print(json.dumps(s, indent=2))

#     # assert task_summary.logged_values["logged-key"] == LoggedValueContent(
#     #     type="utf-8", content="logged-value"
#     # )

#     # assert False
