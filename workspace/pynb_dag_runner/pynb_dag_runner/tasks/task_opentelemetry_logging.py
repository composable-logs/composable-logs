from typing import Any, Optional, Mapping

import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore

import ray
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)


def _log_artefact(name: str, content: str, traceparent: Optional[str] = None):
    tracer = otel.trace.get_tracer(__name__)  # type: ignore

    def log_to_span(span):
        span.set_attribute("name", name)
        span.set_attribute("encoding", "text/utf8")
        span.set_attribute("content", content)
        span.set_status(Status(StatusCode.OK))

    if traceparent is None:
        # Log artefact as sub-span in implicit context
        with tracer.start_as_current_span("artefact") as span:
            log_to_span(span)
    else:
        # Log artefact as sub-span with parent determined from context propagated
        # traceparent.
        context: Mapping[str, str] = TraceContextTextMapPropagator().extract(
            carrier={"traceparent": traceparent}
        )

        with tracer.start_span(
            "artefact",
            context=context,
        ) as span:
            log_to_span(span)


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
        _log_artefact(name, content, traceparent=self._traceparent)
