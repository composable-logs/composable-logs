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
        context = TraceContextTextMapPropagator().extract(
            carrier={"traceparent": traceparent}
        )

        with tracer.start_span(
            "artefact",
            context=context,
        ) as span:
            log_to_span(span)


def connect_to_ray():
    """
    Connect to Ray cluster from eg notebook
    """
    return ray.init(
        address="auto",
        namespace="pydar-ray-cluster",
    )


class PydarLogger:
    """
    pynb-dag-runner client to use from eg notebooks that connects to PydarLoggingActor
    """

    def __init__(self, P):
        assert isinstance(P, dict)

        # Without this, OpenTelemetry in notebook will not connect to Ray-cluster's
        # logging setup.
        connect_to_ray()

        self._traceparent = P.get("_opentelemetry_traceparent", None)

    def log_artefact(self, name, content):
        _log_artefact(name, content, traceparent=self._traceparent)
