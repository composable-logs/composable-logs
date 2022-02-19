import base64, json
from typing import Any, Callable, Optional, Mapping, Union
from dataclasses import dataclass

#
import opentelemetry as otel
from opentelemetry.trace.span import Span
from opentelemetry.trace import StatusCode, Status  # type: ignore

import ray
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

# ---- encode/decode functions -----

LoggableTypes = Union[str, bytes]


@dataclass
class SerializedData:
    # eg "str", "bytes", see LoggableTypes
    type: str

    # "base64" for binary, "utf-8" for string, "json" for other types
    encoding: str

    # encoded data represented as utf-8 string
    content: str

    def decode(self):
        if not isinstance(self.content, str):
            raise ValueError(f"Expected serialized data in utf-8 format.")

        if self.type == "utf-8" and self.encoding == "utf-8":
            return self.content
        elif self.type == "bytes" and self.encoding == "base64":
            return base64.b64decode(self.content)
        else:
            raise ValueError(f"Unknown encoding {self.type}, {self.encoding}.")

    @classmethod
    def encode(cls, content: LoggableTypes) -> "SerializedData":
        if isinstance(content, str):
            return cls("utf-8", "utf-8", content)

        elif isinstance(content, bytes):
            # TODO: review
            # https://docs.python.org/3/library/base64.html#security-considerations
            return cls("bytes", "base64", base64.b64encode(content).decode("utf-8"))
        else:
            raise ValueError("Input should be utf-8 (Python) string or binary")


# ----


def _call_in_trace_context(
    f: Callable[[Span], None], span_name: str, traceparent: Optional[str] = None
):
    """
    Executing a code block `f: Span -> None`in a new OpenTelemetry span with
    name `span_name`. The argument to f is the new span.

    `traceparent`:
      - if None, use current global span context.
      - Otherwise, an explicit span-context can be provided (with context propagated
        using TraceContextTextMapPropagator).
    """
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


def _log_artefact(
    name: str,
    content: Union[bytes, str],
    traceparent: Optional[str] = None,
):
    def _log(span: Span):
        span.set_attribute("name", name)

        serialized_data = SerializedData.encode(content)
        span.set_attribute("type", serialized_data.type)
        span.set_attribute("encoding", serialized_data.encoding)
        span.set_attribute("content", serialized_data.content)

        span.set_status(Status(StatusCode.OK))

    _call_in_trace_context(f=_log, span_name="artefact", traceparent=traceparent)


def _log_named_value(
    name: str, value: Any, encoding: str, traceparent: Optional[str] = None
):
    if not isinstance(name, str):
        raise ValueError(f"name {name} should be string when logging a named-value")

    if value != json.loads(json.dumps(value)):
        raise ValueError("Value should be json-serializable when logging name-value")

    def _log(span: Span):
        span.set_attribute("name", name)
        span.set_attribute("encoding", encoding)
        span.set_attribute("value", json.dumps(value))
        span.set_status(Status(StatusCode.OK))

    _call_in_trace_context(f=_log, span_name="named-value", traceparent=traceparent)


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

    def log_artefact(self, name: str, content: Union[bytes, str]):
        _log_artefact(name=name, content=content, traceparent=self._traceparent)

    def log_value(self, name: str, value: Any):
        """
        Log generic json-serializiable value
        """
        _log_named_value(
            name=name, value=value, encoding="json", traceparent=self._traceparent
        )

    def log_string(self, name: str, value: str):
        if not isinstance(value, str):
            raise ValueError(f"log_string: value not a string {str(value)}")

        _log_named_value(
            name=name,
            value=value,
            encoding="json/string",
            traceparent=self._traceparent,
        )

    def log_int(self, name: str, value: int):
        if not isinstance(value, int):
            raise ValueError(f"log_int: value not an integer {str(value)}")

        _log_named_value(
            name=name, value=value, encoding="json/int", traceparent=self._traceparent
        )

    def log_boolean(self, name: str, value: bool):
        if not isinstance(value, bool):
            raise ValueError(f"log_boolean: value not an boolean {str(value)}")

        _log_named_value(
            name=name, value=value, encoding="json/bool", traceparent=self._traceparent
        )

    def log_float(self, name: str, value: float):
        if not isinstance(value, float):
            raise ValueError(f"log_float: value not a float {str(value)}")

        _log_named_value(
            name=name, value=value, encoding="json/float", traceparent=self._traceparent
        )
