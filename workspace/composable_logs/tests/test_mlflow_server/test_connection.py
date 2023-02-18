import random, time, requests

# -
import pytest


# -
from composable_logs.helpers import one
from composable_logs.opentelemetry_task_span_parser import ArtifactContent
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
from composable_logs.opentelemetry_task_span_parser import LoggedValueContent


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


def get_test_spans_for_two_parallel_tasks():
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
    spans: Spans = get_test_spans_for_two_parallel_tasks()

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


LOG_PARAMETER_TEST_VALUES = {
    # (What is logged into ML Flow client, what is expected in logs)
    "a-string-string-x": ("x", "x"),
    "a-string-int-123": (123, "123"),
    "a-string-float-123.4": (123.4, "123.4"),
    "a-string-int-list-1-2-3": ([1, 2, 3], "[1, 2, 3]"),
}


def get_test_spans_with_different_inputs():
    @task(task_id="ml_flow_data_generator")
    def ml_flow_data_generator():
        configure_mlflow_connection_variables()

        import mlflow

        # currently no run is active
        assert mlflow.active_run() is None

        # --- generate data to mlflow.log_param API ---
        for k, v in LOG_PARAMETER_TEST_VALUES.items():
            logged_value, expected_value_in_logs = v
            mlflow.log_param(k, logged_value)

        # --- generate data to mlflow.log_params API ---
        # uses batch ingestion API
        mlflow.log_params({"aaa": 123, "bbb": [1, 23]})

        # --- tag run, loged as a parameter but with a tags. prefix ---
        mlflow.set_tag("version", "2.3.4")

        # --- generate data to mlflow.log_text API ---
        mlflow.log_text("## Hello \nWorld 😊", "README.md")
        from composable_logs.wrappers import _get_traceparent

        # --- mlflow.active_run() should now is populated ---
        active_run_dict = mlflow.active_run().to_dictionary()
        assert active_run_dict["info"]["run_id"] == _get_traceparent()

        for api_uri in (active_run_dict["info"]["artifact_uri"],):
            assert isinstance(api_uri, str)
            assert api_uri.startswith("ftp://")
            assert _get_traceparent() in api_uri

    with SpanRecorder() as rec:
        run_dag(ml_flow_data_generator())

    return rec.spans


def test_mlflow_logging_for_different_endpoints_with_mix_test_data(mlflow_server):
    spans: Spans = get_test_spans_with_different_inputs()

    workflow_summary = parse_spans(spans)
    assert workflow_summary.is_success()

    for task_summary in [one(workflow_summary.task_runs)]:  # type: ignore
        assert task_summary.task_id == "ml_flow_data_generator"

        # --- check data logged to mlflow.log_param API ---
        for k, v in LOG_PARAMETER_TEST_VALUES.items():
            logged_value, expected_value_in_logs = v
            assert task_summary.logged_values[k] == LoggedValueContent(
                type="utf-8", content=expected_value_in_logs
            )

        # --- check data logged to mlflow.log_params API ---
        assert task_summary.logged_values["aaa"] == LoggedValueContent(
            type="utf-8", content="123"
        )
        assert task_summary.logged_values["bbb"] == LoggedValueContent(
            type="utf-8", content="[1, 23]"
        )

        # --- verify tag was recorded
        assert task_summary.logged_values["tags.version"] == LoggedValueContent(
            type="utf-8", content="2.3.4"
        )

        print(100 * "/", task_summary.logged_artifacts)
        assert task_summary.get_artifact("README.md") == ArtifactContent(
            name="README.md",
            type="bytes",
            content="## Hello \nWorld 😊".encode("utf-8"),
        )


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
