import json
from typing import Any, Callable, Optional, Mapping

#
import opentelemetry as otel
from opentelemetry.trace.span import Span
from opentelemetry.trace import StatusCode, Status  # type: ignore

import ray
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)


def _call_in_trace_context(
    f: Callable[[Span], None], span_name: str, traceparent: Optional[str] = None
):
    tracer = otel.trace.get_tracer(__name__)  # type: ignore

    if traceparent is None:
        # Log artefact as sub-span in implicit context
        with tracer.start_as_current_span(span_name) as span:
            f(span)
    else:
        # Log artefact as sub-span with parent determined from context propagated
        # traceparent.
        context: Mapping[str, str] = (
            TraceContextTextMapPropagator()
            # -
            .extract(carrier={"traceparent": traceparent})
        )

        with tracer.start_span(span_name, context=context) as span:
            f(span)


def _log_artefact(name: str, content: str, traceparent: Optional[str] = None):
    def _log(span: Span):
        span.set_attribute("name", name)
        span.set_attribute("encoding", "text/utf8")
        span.set_attribute("content", content)
        span.set_status(Status(StatusCode.OK))

    _call_in_trace_context(f=_log, span_name="artefact", traceparent=traceparent)


def _log_key_value(key: str, value: Any, traceparent: Optional[str] = None):
    if not isinstance(key, str):
        raise ValueError("Key should be string when logging key-value")

    if value != json.loads(json.dumps(value)):
        raise ValueError("Value should be json-serializable when logging key-value")

    def _log(span: Span):
        span.set_attribute("key", key)
        span.set_attribute("value-json", json.dumps(value))
        span.set_status(Status(StatusCode.OK))

    _call_in_trace_context(f=_log, span_name="key-value", traceparent=traceparent)


class PydarLogger:
    """
    pynb-dag-runner logger that can be used eg notebooks
    """

    def __init__(self, P: Mapping[str, Any]):
        assert isinstance(P, dict)

        # Connect to running Ray cluster. Without this, OpenTelemetry logging from
        #  notebook will not connect to Ray-cluster's logging setup.
        ray.init(address="auto", namespace="pydar-ray-cluster")

        # Get context for Task that triggered notebook (for context propagation)
        self._traceparent = P.get("_opentelemetry_traceparent", None)

    def log_artefact(self, name: str, content: str):
        _log_artefact(name=name, content=content, traceparent=self._traceparent)

    def log_key_value(self, key: str, value: Any):
        _log_key_value(key=key, value=value, traceparent=self._traceparent)
