import time

#
import pytest

#
import opentelemetry as ot

#
from pynb_dag_runner.opentelemetry_helpers import (
    read_key,
    is_parent_child,
    get_duration_s,
    get_span_exceptions,
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.helpers import one


@pytest.mark.parametrize("dummy_loop_parameter", range(3))
def test__otel__spans__tracing_native_python(dummy_loop_parameter):
    with SpanRecorder() as r:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("TopLevel") as t:
            t.record_exception(ValueError("foo!"))

    span = one(r.spans)
    assert read_key(span, ["name"]) == "TopLevel"
    assert read_key(span, ["parent_id"]) is None

    exception_event = one(get_span_exceptions(span))
    assert exception_event["name"] == "exception"
    assert exception_event["attributes"]["exception.type"] == "ValueError"
    assert exception_event["attributes"]["exception.message"] == "foo!"


@pytest.mark.parametrize("should_fail", [True, False])
def test__otel__spans__exceptions_events(should_fail):
    with SpanRecorder() as r:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("TopLevel") as t:
            if should_fail:
                t.record_exception(ValueError("foo!"))

    if should_fail:
        exception = one(r.spans.exception_events())
        assert exception.keys() == set(["attributes", "name", "timestamp"])
        assert exception["name"] == "exception"
        assert exception["attributes"]["exception.type"] == "ValueError"
        assert exception["attributes"]["exception.message"] == "foo!"

    else:
        assert len(r.spans.exception_events()) == 0


@pytest.mark.parametrize("dummy_loop_parameter", range(3))
def test__otel__spans__tracing_nested_native_python(dummy_loop_parameter):
    # test that we can record and validate properties of spans emitted by native
    # Python code

    def get_test_spans():
        with SpanRecorder() as sr:
            tracer = ot.trace.get_tracer(__name__)

            with tracer.start_as_current_span("top"):
                time.sleep(0.2)
                with tracer.start_as_current_span("sub1") as span_sub1:
                    time.sleep(0.3)
                    span_sub1.set_attribute("sub1attribute", 12345)
                    with tracer.start_as_current_span("sub11"):
                        time.sleep(0.4)

                with tracer.start_as_current_span("sub2"):
                    time.sleep(0.1)

        return sr.spans

    spans: Spans = get_test_spans()

    assert len(spans) == 4

    top = one(spans.filter(["name"], "top"))
    sub1 = one(spans.filter(["name"], "sub1"))
    sub11 = one(spans.filter(["name"], "sub11"))
    sub2 = one(spans.filter(["name"], "sub2"))

    def check_parent_child(parent, child):
        assert spans.contains_path(parent, child, recursive=True)
        assert spans.contains_path(parent, child, recursive=False)
        assert is_parent_child(parent, child)

    check_parent_child(top, sub1)
    check_parent_child(top, sub2)
    check_parent_child(sub1, sub11)

    assert not is_parent_child(top, top)
    assert not is_parent_child(sub1, top)

    # Check that we can find "top -> sub11" relationship in "top -> sub1 -> sub11"
    assert spans.contains_path(top, sub11, recursive=True)
    assert not spans.contains_path(top, sub11, recursive=False)

    def check_duration(span, expected_duration_s: float) -> bool:
        return abs(get_duration_s(span) - expected_duration_s) < 0.05

    assert check_duration(top, 0.2 + 0.3 + 0.4 + 0.1)
    assert check_duration(sub1, 0.3 + 0.4)
    assert check_duration(sub11, 0.4)
    assert check_duration(sub2, 0.1)

    assert read_key(sub1, ["attributes", "sub1attribute"]) == 12345
