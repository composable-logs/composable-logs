import opentelemetry as ot
from pathlib import Path
from typing import List

#
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder
from pynb_dag_runner.helpers import one
from pynb_dag_runner.tasks.tasks import _get_traceparent
from pynb_dag_runner.tasks.task_opentelemetry_logging import (
    PydarLogger,
    SerializedData,
    LoggableTypes,
    get_logged_values,
    get_logged_artifacts,
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
        with SpanRecorder() as r:
            tracer = ot.trace.get_tracer(__name__)
            with tracer.start_as_current_span("parent-span") as t1:
                with tracer.start_as_current_span("sub-span") as t2:
                    logger = PydarLogger(
                        P={
                            "_opentelemetry_traceparent": _get_traceparent(),
                        }
                    )
                    logger.log_int("name", 1000)

        return r.spans

    def validate_spans(spans: Spans):
        assert len(spans) == 3

        top_span = one(spans.filter(["name"], "parent-span"))
        sub_span = one(spans.filter(["name"], "sub-span"))
        log_span = one(spans.filter(["name"], "named-value"))

        assert spans.contains_path(top_span, sub_span, log_span)

    validate_spans(get_test_spans())


class _fig:
    # dummy mock of matplotlib figure object for testing
    def savefig(self, file_name, **kw_args):
        Path(file_name).write_bytes(bytes([23, 34]))


@pytest.mark.parametrize(
    "test_case",
    [
        # -- logged values --
        {
            "name": "x",
            "value": 1.23,
            "log_named_value": lambda logger: logger.log_float,
            "get_logged_named_values": get_logged_values,
        },
        {
            "name": "y",
            "value": 123,
            "log_named_value": lambda logger: logger.log_int,
            "get_logged_named_values": get_logged_values,
        },
        {
            "name": "z",
            "value": True,
            "log_named_value": lambda logger: logger.log_boolean,
            "get_logged_named_values": get_logged_values,
        },
        {
            "name": "u",
            "value": "foo123",
            "log_named_value": lambda logger: logger.log_string,
            "get_logged_named_values": get_logged_values,
        },
        {
            "name": "v",
            "value": {"a": 123, "b": None},
            "log_named_value": lambda logger: logger.log_value,
            "get_logged_named_values": get_logged_values,
        },
        # -- logged files --
        {
            "name": "file.txt",
            "value": "utf-8-file-content-EOF",
            "log_named_value": lambda logger: logger.log_artefact,
            "get_logged_named_values": get_logged_artifacts,
        },
        {
            "name": "file.bin",
            "value": bytes([0, 1]),
            "log_named_value": lambda logger: logger.log_artefact,
            "get_logged_named_values": get_logged_artifacts,
        },
        {
            "name": "file.png",
            "f": lambda logger: logger.log_figure("file.png", _fig()),
            "value": bytes([23, 34]),
            "log_named_value": lambda logger: lambda name, _: logger.log_figure(
                name, _fig()
            ),
            "get_logged_named_values": get_logged_artifacts,
        },
    ],
)
def test__pydar_logger__logged_span_values_and_artifacts(test_case):
    get_logged_named_values = test_case["get_logged_named_values"]
    log_named_value = test_case["log_named_value"]
    name = test_case["name"]
    value = test_case["value"]

    def get_test_spans():
        with SpanRecorder() as r:
            tracer = ot.trace.get_tracer(__name__)
            with tracer.start_as_current_span("parent-span") as t1:
                logger = PydarLogger(
                    P={
                        "_opentelemetry_traceparent": _get_traceparent(),
                    }
                )
                log_named_value(logger)(name, value)

        return r.spans

    def validate_spans(spans: Spans):
        assert len(spans) == 2

        data = get_logged_named_values(spans)
        assert data.keys() == set([name])
        assert data[name] == value

    validate_spans(get_test_spans())
