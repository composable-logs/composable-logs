import random, time
from pathlib import Path

# -
import pytest
import mlflow


# -
from composable_logs.helpers import one, Try
from composable_logs.opentelemetry_task_span_parser import ArtifactContent
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.opentelemetry_task_span_parser import parse_spans
from composable_logs.wrappers import task, run_dag
from composable_logs.wrappers import _get_traceparent
from composable_logs.mlflow_server.server import (
    configure_mlflow_connection_variables,
    mlflow_server_is_running,
    ensure_mlflow_server_is_running,
    shutdown_mlflow_server,
)
from composable_logs.opentelemetry_task_span_parser import LoggedValueContent


@pytest.fixture(scope="module", autouse=True)
def mlflow_server():
    ensure_mlflow_server_is_running()
    yield
    shutdown_mlflow_server()


def test_mlflow_server_is_running():
    assert mlflow_server_is_running()


def get_test_spans_for_two_parallel_tasks():
    # Run two tasks in parallel and check that ML Flow data is not mixed up between
    # tasks.

    @task(task_id="ml_flow_tester_1")
    def ml_flow_tester_1():
        configure_mlflow_connection_variables()

        for k in range(10):
            time.sleep(random.random() * 0.10)
            mlflow.log_param(f"1-logged-key-{k}", "some logged parameter value")
            mlflow.log_text("some-file-content", f"1-file-{k}")

    @task(task_id="ml_flow_tester_2")
    def ml_flow_tester_2():
        configure_mlflow_connection_variables()
        for k in range(10):
            time.sleep(random.random() * 0.10)
            mlflow.log_param(f"2-logged-key-{k}", "some logged parameter value")
            mlflow.log_text("some-file-content", f"2-file-{k}")

    with SpanRecorder() as rec:
        run_dag([ml_flow_tester_1(), ml_flow_tester_2()])

    return rec.spans


def test_mlflow_data_from_parallel_tasks_are_split_correctly():
    spans: Spans = get_test_spans_for_two_parallel_tasks()

    nr_loops = 10

    workflow_summary = parse_spans(spans)
    assert workflow_summary.is_success()

    for task_summary in workflow_summary.task_runs:  # type: ignore
        assert len(task_summary.logged_artifacts) == nr_loops
        assert len(task_summary.logged_values) == nr_loops

        assert task_summary.task_id in ["ml_flow_tester_1", "ml_flow_tester_2"]
        if task_summary.task_id == "ml_flow_tester_1":
            assert task_summary.logged_values.keys() == {
                f"1-logged-key-{k}" for k in range(nr_loops)
            }
            assert set([a.name for a in task_summary.logged_artifacts]) == {
                f"1-file-{k}" for k in range(nr_loops)
            }
        if task_summary.task_id == "ml_flow_tester_2":
            assert task_summary.logged_values.keys() == {
                f"2-logged-key-{k}" for k in range(nr_loops)
            }

            assert set([a.name for a in task_summary.logged_artifacts]) == {
                f"2-file-{k}" for k in range(nr_loops)
            }


LOG_PARAMETER_TEST_VALUES = {
    # (What is logged into ML Flow client, what is expected in logs)
    "a-string-string-x": ("x", "x"),
    "a-string-int-123": (123, "123"),
    "a-string-float-123.4": (123.4, "123.4"),
    "a-string-int-list-1-2-3": ([1, 2, 3], "[1, 2, 3]"),
}


def get_test_spans_with_different_inputs(tmp_path: Path):
    @task(task_id="ml_flow_data_generator")
    def ml_flow_data_generator():
        configure_mlflow_connection_variables()

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

        # --- log files in local directory ---
        (tmp_path / "stdout.txt").write_text("log-line")
        (tmp_path / "vars.txt").write_text("x, y, t")
        mlflow.log_artifacts(str(tmp_path), artifact_path="artifacts")

        # --- log metrics ---
        mlflow.log_metric("a-logged-metric", 12.900)

        # Not tested, but the below SDK methods should work since they should call
        # the same server endpoints as used above:
        #
        #  - mlflow.log_dict
        #  - mlflow.log_artifact
        #  - mlflow.log_figure
        #  - mlflow.log_image
        #  - mlflow.log_metrics (does not work if it calles batch metric ingestion API)

        # --- mlflow.active_run() should now is populated ---
        for run_dict in [
            mlflow.active_run().to_dictionary(),
            mlflow.last_active_run().to_dictionary(),
        ]:
            assert run_dict["info"]["run_id"] == _get_traceparent()

            for api_uri in (run_dict["info"]["artifact_uri"],):
                assert isinstance(api_uri, str)
                assert api_uri.startswith("ftp://")
                assert _get_traceparent() in api_uri

    with SpanRecorder() as rec:
        run_dag(ml_flow_data_generator())

    return rec.spans


def test_mlflow_logging_for_different_endpoints_with_mix_test_data(tmp_path: Path):
    spans: Spans = get_test_spans_with_different_inputs(tmp_path)

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

        # --- metrics currently stored as parameters ---
        assert task_summary.logged_values["a-logged-metric"] == LoggedValueContent(
            type="float", content=12.900
        )

        # --- verify tag was recorded
        assert task_summary.logged_values["tags.version"] == LoggedValueContent(
            type="utf-8", content="2.3.4"
        )

        assert task_summary.get_artifact("README.md") == ArtifactContent(
            name="README.md",
            type="bytes",
            content="## Hello \nWorld 😊".encode("utf-8"),
        )

        assert task_summary.get_artifact("artifacts/stdout.txt") == ArtifactContent(
            name="artifacts/stdout.txt",
            type="bytes",
            content="log-line".encode("utf-8"),
        )

        assert task_summary.get_artifact("artifacts/vars.txt") == ArtifactContent(
            name="artifacts/vars.txt",
            type="bytes",
            content="x, y, t".encode("utf-8"),
        )


def test_mlflow_client_crashes_for_unsupported_api_calls():
    @task(task_id="test_unsupported_api_calls")
    def test_unsupported_api_calls():
        configure_mlflow_connection_variables()

        # TODO: Not sure why a try .. except is neded here. Without these we
        # get a timeout exception (?!)
        try:
            mlflow.get_experiment(experiment_id="this-does-not-exist")
            raise Exception("An exception should have been thrown")

        except Exception as e:
            return str(e)

    with SpanRecorder() as rec:
        result: Try[str] = run_dag(test_unsupported_api_calls())

        assert result.is_success()
        assert "GET api/2.0/mlflow/experiments/get not supported" in str(result.value)


@pytest.mark.parametrize("mlflow_logger", [0, 1, 2])
def test_mlflow_different_call_approaches(mlflow_logger):
    @task(task_id="mlflow-1")
    def ml_flow_data_generator():
        configure_mlflow_connection_variables()

        if mlflow_logger == 0:
            mlflow.log_param("abc", "123")
        elif mlflow_logger == 1:
            mlflow.start_run()
            mlflow.log_param("abc", "123")
            mlflow.end_run()
        elif mlflow_logger == 2:
            with mlflow.start_run() as run:
                mlflow.log_param("abc", "123")

    with SpanRecorder() as rec:
        run_dag(ml_flow_data_generator())

    workflow_summary = parse_spans(rec.spans)
    assert workflow_summary.is_success()

    for task_summary in [one(workflow_summary.task_runs)]:  # type: ignore
        assert len(task_summary.logged_values) == 1

        assert task_summary.logged_values.keys() == {"abc"}


def test_mlflow_nested_runs_should_fail():
    @task(task_id="nested-mlflow-runs")
    def ml_flow_data_generator():
        configure_mlflow_connection_variables()

        with mlflow.start_run() as run1:
            mlflow.log_param("pqr", "3000")
            with mlflow.start_run(nested=True) as run2:
                mlflow.log_param("rs", "4000")

    with SpanRecorder() as rec:
        run_dag(ml_flow_data_generator())

    workflow_summary = parse_spans(rec.spans)
    for task_summary in [one(workflow_summary.task_runs)]:  # type: ignore
        assert len(task_summary.logged_values) == 1
        assert task_summary.logged_values.keys() == {"pqr"}
        assert "nested runs are not supported" in str(task_summary.exceptions)
