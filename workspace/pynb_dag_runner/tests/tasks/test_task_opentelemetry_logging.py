import opentelemetry as ot
from pathlib import Path

#
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder
from pynb_dag_runner.helpers import one
from pynb_dag_runner.tasks.tasks import _get_traceparent
from pynb_dag_runner.opentelemetry_task_span_parser import _decode_data_content_span
from pynb_dag_runner.tasks.task_opentelemetry_logging import PydarLogger, SerializedData

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
    for msg in [
        "test-text-message",
        bytes([0, 1, 2, 3]),
        bytes(1000 * list(range(256))),
        True,
        1.23,
        1000000,
        {"a": 1, "b": [None, {"c": True}]},
    ]:
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


def _make_log(type, name, value):
    return {
        "name": name,
        "type": type,
        "content": value,
    }


class _fig:
    def savefig(self, file_name, **kw_args):
        Path(file_name).write_bytes(bytes([23, 34]))


@pytest.mark.parametrize(
    "test_case",
    [
        # -- logged values --
        {
            "f": lambda logger: logger.log_float("x", 1.23),
            "expected_log": _make_log(type="float", name="x", value=1.23),
        },
        {
            "f": lambda logger: logger.log_int("y", 123),
            "expected_log": _make_log(type="int", name="y", value=123),
        },
        {
            "f": lambda logger: logger.log_boolean("z", True),
            "expected_log": _make_log(type="bool", name="z", value=True),
        },
        {
            "f": lambda logger: logger.log_string("u", "foo123"),
            "expected_log": _make_log(type="utf-8", name="u", value="foo123"),
        },
        {
            "f": lambda logger: logger.log_value("v", {"a": 123, "b": None}),
            "expected_log": _make_log(
                type="json", name="v", value={"a": 123, "b": None}
            ),
        },
        {
            "f": lambda logger: logger.log_artefact("file.txt", "EOF"),
            "expected_log": _make_log(type="utf-8", name="file.txt", value="EOF"),
        },
        # -- logged files --
        {
            "f": lambda logger: logger.log_artefact("file.bin", bytes([0, 1])),
            "expected_log": _make_log(
                type="bytes", name="file.bin", value=bytes([0, 1])
            ),
        },
        {
            "f": lambda logger: logger.log_figure("file.png", _fig()),
            "expected_log": _make_log(
                type="bytes", name="file.png", value=bytes([23, 34])
            ),
        },
    ],
)
def test__pydar_logger__logged_spans(test_case):
    def get_test_spans():
        with SpanRecorder() as r:
            tracer = ot.trace.get_tracer(__name__)
            with tracer.start_as_current_span("parent-span") as t1:
                logger = PydarLogger(
                    P={
                        "_opentelemetry_traceparent": _get_traceparent(),
                    }
                )
                test_case["f"](logger)

        return r.spans

    def validate_spans(spans: Spans):
        assert len(spans) == 2

        top_span = one(spans.filter(["name"], "parent-span"))
        log_span = one(
            list(spans.filter(["name"], "named-value"))
            + list(spans.filter(["name"], "artefact"))
        )

        assert spans.contains_path(top_span, log_span, recursive=False)

        assert _decode_data_content_span(log_span) == test_case["expected_log"]

    validate_spans(get_test_spans())
