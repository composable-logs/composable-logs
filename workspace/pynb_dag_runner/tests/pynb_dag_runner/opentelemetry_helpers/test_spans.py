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
        assert spans.contains_path(parent, child)
        assert is_parent_child(parent, child)

    check_parent_child(top, sub1)
    check_parent_child(top, sub2)
    check_parent_child(sub1, sub11)

    assert not is_parent_child(top, top)
    assert not is_parent_child(sub1, top)

    # Check that we can find "top -> sub11" relationship in "top -> sub1 -> sub11"
    assert spans.contains_path(top, sub11)

    def check_duration(span, expected_duration_s: float) -> bool:
        return abs(get_duration_s(span) - expected_duration_s) < 0.05

    assert check_duration(top, 0.2 + 0.3 + 0.4 + 0.1)
    assert check_duration(sub1, 0.3 + 0.4)
    assert check_duration(sub11, 0.4)
    assert check_duration(sub2, 0.1)

    assert read_key(sub1, ["attributes", "sub1attribute"]) == 12345


def test__otel__spans__get_attributes__empty_span_collection():
    assert Spans([]).get_attributes() == {}


def test__otel__spans__get_attributes__with_unique_attribute_values():
    with SpanRecorder() as r:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("TopLevel") as t1:
            t1.set_attribute("a.foo", "1")
            with tracer.start_as_current_span("SubLevel") as t2:
                t2.set_attribute("a.foo", "1")
                t2.set_attribute("b.bar", "2")

    assert r.spans.get_attributes(allowed_prefixes={"a"}) == {"a.foo": "1"}
    assert r.spans.get_attributes(allowed_prefixes={"b.bar"}) == {"b.bar": "2"}
    assert r.spans.get_attributes(allowed_prefixes={"not-found-prefix"}) == {}
    assert r.spans.get_attributes(allowed_prefixes={}) == {}
    assert (
        r.spans.get_attributes(allowed_prefixes={""})
        == r.spans.get_attributes()
        == {"a.foo": "1", "b.bar": "2"}
    )


def test__otel__spans__get_attributes__with_non_unique_attribute_values():
    with SpanRecorder() as r:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("span-1") as t1:
            t1.set_attribute("a.foo", "1")
            t1.set_attribute("b.bar", "2")
            t1.set_attribute("c.baz", "3")

        with tracer.start_as_current_span("span-2") as t2:
            t2.set_attribute("a.foo", "2")  # not-unique, previously a.foo = "1"
            t2.set_attribute("b.bar", "2")

    # even if span collection contain non-unique attribute keys we can extract unions
    # when key-values are unique after filtering
    assert r.spans.get_attributes(allowed_prefixes={"b"}) == {"b.bar": "2"}
    assert r.spans.get_attributes(allowed_prefixes={"c"}) == {"c.baz": "3"}

    # check that get_attributes crashes if key-values are not unique
    for allowed_prefixes in [None, {"a.foo"}]:
        with pytest.raises(ValueError):
            _ = r.spans.get_attributes(allowed_prefixes=allowed_prefixes)


def test__otel__demo_context_propagation_mechanism():
    # This unit test is demo:ing of principle how OpenTelemetry span
    # contexts are propagated to notebooks.
    #
    # Context propagation using TraceContextTextMapPropagator as shown here:
    # - https://github.com/open-telemetry/opentelemetry-python/blob/main/docs/faq-and-cookbook.rst
    # - https://stackoverflow.com/questions/68494838

    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )

    def get_test_spans():
        with SpanRecorder() as r:
            tracer1 = ot.trace.get_tracer("tracer1")

            with tracer1.start_as_current_span("parent-span") as t1:
                t1.set_attribute("parent-attr", "foo")

                # Get Span context as a string that can be propagated
                # to other processes.
                ctx_propagator1 = TraceContextTextMapPropagator()
                carrier = {}
                ctx_propagator1.inject(carrier=carrier)
                assert isinstance(carrier, dict)
                assert carrier.keys() == {"traceparent"}
                assert isinstance(carrier["traceparent"], str)

            del tracer1, t1

            # Create sub-span based only on data in context propagation carrier dict.
            # This could run in some other process (eg. remote Ray task) or call to
            # service behind REST API.
            tracer2 = ot.trace.get_tracer("tracer2-in-some-other-process")
            ctx_propagator2 = TraceContextTextMapPropagator()
            with tracer2.start_span(
                "child-span", context=ctx_propagator2.extract(carrier=carrier)
            ) as child:
                child.set_attribute("child-attr", "foo-bar")

        return r.spans

    def validate_spans(spans: Spans):
        assert len(spans) == 2

        top_span = one(spans.filter(["name"], "parent-span"))
        child_span = one(spans.filter(["name"], "child-span"))

        assert top_span["attributes"]["parent-attr"] == "foo"
        assert child_span["attributes"]["child-attr"] == "foo-bar"

        assert spans.contains_path(top_span, child_span)

    validate_spans(get_test_spans())
