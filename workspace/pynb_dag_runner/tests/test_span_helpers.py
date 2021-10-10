from .span_test_helper import SpanRecorder

#
import pytest
import opentelemetry as ot

#
from pynb_dag_runner.opentelemetry_helpers import read_key


@pytest.mark.parametrize("dummy_loop_parameter", range(3))
def test_tracing_native_python(dummy_loop_parameter):
    with SpanRecorder() as sr:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("TopLevel") as t:
            pass

    assert len(sr.spans) == 1

    for span in sr.spans:
        assert read_key(span, ["name"]) == "TopLevel"
        assert read_key(span, ["parent_id"]) is None
