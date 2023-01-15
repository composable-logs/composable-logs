import opentelemetry as ot
from pathlib import Path
from typing import List

#
from pynb_dag_runner.wrappers import task, run_dag, TaskContext
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder
from pynb_dag_runner.helpers import one, Success
from pynb_dag_runner.tasks.tasks import _get_traceparent
from pynb_dag_runner.tasks.task_opentelemetry_logging import (
    PydarLogger,
    SerializedData,
    LoggableTypes,
)

from pynb_dag_runner.opentelemetry_task_span_parser import (
    parse_spans,
    LoggedValueContent,
    ArtifactContent,
)

# --
import pytest

# ---- test SerializedData encoding and decoding ----


@pytest.mark.parametrize(
    "test_case",
    [
        ("foo", SerializedData("utf-8", "utf-8", "foo")),
        (123, SerializedData("int", "json", "123")),
        (bytes([0, 1, 2, 3, 4, 5]), SerializedData("bytes", "base64", "AAECAwQF")),
    ],
)
def test__encode_decode_to_wire__explicit_examples(test_case):
    data, serialized_data = test_case

    assert SerializedData.encode(data) == serialized_data
    assert serialized_data.decode() == data


def test__encode_decode_to_wire__is_identity():
    test_messages: List[LoggableTypes] = [
        "test-text-message",
        bytes([0, 1, 2, 3]),
        bytes(1000 * list(range(256))),
        True,
        1.23,
        1000000,
        {"a": 1, "b": [None, {"c": True}]},
    ]
    for msg in test_messages:
        assert msg == SerializedData.encode(msg).decode()


def test__encode_decode_to_wire__exceptions_for_invalid_data():
    class Foo:
        pass

    # encoding/decoding should fail for these inputs
    examples_of_invalid_data = [None, Foo()]

    for invalid_data in examples_of_invalid_data:
        with pytest.raises(Exception):
            SerializedData.encode(invalid_data)

    with pytest.raises(ValueError):
        SerializedData("string", "utf8", "should be 'utf-8'").decode()


# ---- test logging from PydarLogger ----


def test__pydar_logger__logged_spans_are_nested():
    def get_test_spans():
        with SpanRecorder() as rec:
            tracer = ot.trace.get_tracer(__name__)
            with tracer.start_as_current_span("parent-span") as t1:
                with tracer.start_as_current_span("sub-span") as t2:
                    logger = PydarLogger(
                        P={
                            "_opentelemetry_traceparent": _get_traceparent(),
                        }
                    )
                    logger.log_int("name", 1000)

        return rec.spans

    def validate_spans(spans: Spans):
        assert len(spans) == 3

        top_span = one(spans.filter(["name"], "parent-span"))
        sub_span = one(spans.filter(["name"], "sub-span"))
        log_span = one(spans.filter(["name"], "named-value"))

        assert spans.contains_path(top_span, sub_span, log_span)

    validate_spans(get_test_spans())


# --- test logging of values work with Python tasks and Spans parser ---

TEST_MOCK_MATPLOTLIB_PNG = bytes([12, 23, 34, 45, 56, 67, 78, 89, 90])
TEST_BINARY_FILE = bytes(1000 * list(range(256)))
TEST_PYTHON_DICT = {"a": 1, "b": [2, 3, None], "c": 2}


@pytest.fixture(scope="module")
def spans_to_test_otel_loggging() -> Spans:
    #
    #   task-f  ---\
    #               \
    #                +--- task-h
    #               /
    #   task-g  ---/
    #
    #
    # In this setup, tasks f ang g log the same artifact/value names, but with different
    # values. This allows us to test that logging keeps track where a value was logged.
    #
    @task(task_id="task-f")
    def f(C: TaskContext):
        C.log_artefact("read-first", "hello")
        C.log_int("read-first", 111)
        return 1000

    @task(task_id="task-g")
    def g(C: TaskContext):
        C.log_artefact("read-first", TEST_BINARY_FILE)
        C.log_int("read-first", 222)
        return 2000

    @task(task_id="task-h")
    def h(f_output, g_output, C: TaskContext):
        assert f_output == 1000 and g_output == 2000
        C.log_int("a-logged-int", 1020)
        C.log_float("a-logged-float", 12.3)
        # C.log_float("b-float", 12) will fail
        C.log_boolean("a-logged-bool", True)
        C.log_string("a-logged-string", "///")
        C.log_value("a-logged-json-value", TEST_PYTHON_DICT)

        class _mock_fig:
            # dummy mock of matplotlib figure object for testing
            def savefig(self, file_name, **kw_args):
                Path(file_name).write_bytes(TEST_MOCK_MATPLOTLIB_PNG)

        C.log_figure("mock-matplot-lib-figure.png", _mock_fig())

        return 3000

    with SpanRecorder() as rec:
        assert Success(3000) == run_dag(dag=h(f(), g()))

    return rec.spans


def test__pydar_logger__parse_logged_values_from_three_python_tasks(
    spans_to_test_otel_loggging: Spans,
):
    pipeline_summary = parse_spans(spans_to_test_otel_loggging)

    for task_summary in pipeline_summary.task_runs:  # type: ignore

        def check_logged_value(value_name, value_type, value):
            assert task_summary.logged_values[value_name] == LoggedValueContent(
                type=value_type, content=value
            ), f"mismatch: {value_name} ({value_type}) = {value}"

        if task_summary.task_id == "task-f":
            assert one(task_summary.logged_artifacts) == ArtifactContent(
                name="read-first", type="utf-8", content="hello"
            )
            one(task_summary.logged_values)

            check_logged_value("read-first", "int", 111)

        elif task_summary.task_id == "task-g":
            assert one(task_summary.logged_artifacts) == ArtifactContent(
                name="read-first",
                type="bytes",
                content=TEST_BINARY_FILE,
            )
            one(task_summary.logged_values)
            check_logged_value("read-first", "int", 222)

        elif task_summary.task_id == "task-h":
            assert one(task_summary.logged_artifacts) == ArtifactContent(
                name="mock-matplot-lib-figure.png",
                type="bytes",
                content=TEST_MOCK_MATPLOTLIB_PNG,
            )

            assert len(task_summary.logged_values) == 5
            for args in [
                ("a-logged-int", "int", 1020),
                ("a-logged-float", "float", 12.3),
                ("a-logged-bool", "bool", True),
                ("a-logged-string", "utf-8", "///"),
                ("a-logged-json-value", "json", TEST_PYTHON_DICT),
            ]:
                check_logged_value(*args)
        else:
            raise Exception(f"Unknown task-id: {task_summary.task_id}")
