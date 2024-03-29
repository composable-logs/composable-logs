import base64, json, tempfile, os
from pathlib import Path
from typing import Any, Callable, Optional, Mapping, Union, Dict
from dataclasses import dataclass
import time

# -
import pydantic as p
import ray

import opentelemetry as otel
from opentelemetry.trace.span import Span
from opentelemetry.trace import StatusCode, Status  # type: ignore
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

# -
from composable_logs.opentelemetry_helpers import Spans


class TaskParameters(p.BaseModel):
    parameters: Dict[str, Any]

    # values should not be null

    def add(self, a_dict):
        return TaskParameters(parameters={**self.parameters, **a_dict})


def log_parameters(span, p: TaskParameters):
    for k, v in p.parameters.items():
        if v is None:
            # Behavior of NULL OpenTelemetry attributes is undefined.
            # See
            # https://opentelemetry-python.readthedocs.io/en/latest/api/trace.span.html#opentelemetry.trace.span.Span.set_attributes
            raise ValueError("OpenTelemetry attributes should be non-null")
        span.set_attribute(k, v)


@ray.remote(num_cpus=0)
class ParameterActor:
    """
    Actor to hold parameters so they can be passed into task using named actor
    """

    def __init__(self, parameters: TaskParameters):
        self._parameters: TaskParameters = parameters

    def get(self) -> TaskParameters:
        return self._parameters


# --- The functions three functions should be revised (deleted) ---
#
# The function get_logged_values is used by the summary step in mnist demo pipeline.
# This should be refactored to use the same log parser as all other tools. Then
# the log parsing logic is only implemented in one place.
#
# https://github.com/orgs/composable-logs/projects/2/views/3?pane=issue&itemId=18581653

# to be deleted
def _read_logged_serialized_data(spans: Spans, filter_name: str):
    """
    Inverse of _log_named_value; read all logged artifacts/named values from
    a collection of spans.
    The return value is a key-value dictionary with deserialized values.
    If there are multiple values logged under the same name, the last logged value
    is used.
    """
    assert filter_name in ["artefact", "named-value"]

    values = {}
    for s0 in spans.filter(["name"], filter_name).sort_by_start_time(reverse=True):
        data_attr = s0["attributes"]
        value = SerializedData(
            type=data_attr["type"],
            encoding=data_attr["encoding"],
            encoded_content=data_attr["content_encoded"],
        ).decode()
        value_name = data_attr["name"]

        if value_name not in values:
            values[value_name] = value

    return values


# to be deleted
def get_logged_artifacts(spans: Spans) -> Mapping[str, Any]:
    return _read_logged_serialized_data(spans, filter_name="artefact")


# to be deleted
def get_logged_values(spans: Spans) -> Mapping[str, Any]:
    return _read_logged_serialized_data(spans, filter_name="named-value")


# ---- encode/decode functions -----

# TODO: Review if Any/None should be loggable
LoggableTypes = Union[str, bytes, int, float, bool, Any]


@dataclass
class SerializedData:
    # eg "utf-8", "bytes", see LoggableTypes
    type: str

    # "base64" for binary, "utf-8" for string, "json" for other types
    encoding: str

    # encoded data represented as utf-8 string
    encoded_content: str

    def decode(self):
        if not isinstance(self.encoded_content, str):
            raise ValueError(f"Expected serialized data in utf-8 format.")

        if self.type == self.encoding == "utf-8":
            return self.encoded_content
        elif self.type == self.encoding == "json":
            return json.loads(self.encoded_content)
        elif self.type == "bytes" and self.encoding == "base64":
            return base64.b64decode(self.encoded_content)
        elif self.type in ["bool", "float", "int"] and self.encoding == "json":
            return json.loads(self.encoded_content)
        else:
            raise ValueError(f"Unknown encoding {self.type}, {self.encoding}.")

    @classmethod
    def encode(cls, content: LoggableTypes) -> "SerializedData":
        if isinstance(content, str):
            return cls("utf-8", "utf-8", content)

        # TODO
        if content is None:
            raise ValueError("Logging null values not supported")

        if isinstance(content, bytes):
            # TODO: review
            # https://docs.python.org/3/library/base64.html#security-considerations
            return cls("bytes", "base64", base64.b64encode(content).decode("utf-8"))

        try:
            json_data = json.dumps(content)
            if isinstance(content, bool):
                # Note: isinstance(True, int) == True, so we first test for bool
                return cls("bool", "json", json_data)
            elif isinstance(content, int):
                return cls("int", "json", json_data)
            elif isinstance(content, float):
                return cls("float", "json", json_data)
            else:
                # input data is JSON serializable (json.dumps does not crash)
                return cls("json", "json", json_data)
        except Exception as e:
            raise RuntimeError(f"Unable to parse {str(content)[:1000]}") from e


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
        from opentelemetry.trace.propagation.tracecontext import Context

        context: Context = (
            TraceContextTextMapPropagator()
            # -
            .extract(carrier={"traceparent": traceparent})
        )

        with tracer.start_span(span_name, context=context) as span:
            f(span)


def _log_named_value(
    name: str,
    content: Any,
    content_type: str,
    is_file: bool = False,
    traceparent: Optional[str] = None,
):
    # TODO: content_type not used; but would be required to handle typed null values

    if not isinstance(name, str):
        raise ValueError(f"name {name} should be string when logging a named-value")

    if content_type not in ["bytes", "utf-8"]:
        print(f" - Logging {name} ({content_type}) :", str(content)[:1000])

    def _log(span: Span):
        span.set_attribute("name", name)

        serialized_data = SerializedData.encode(content)

        span.set_attribute("type", serialized_data.type)
        span.set_attribute("encoding", serialized_data.encoding)
        span.set_attribute("content_encoded", serialized_data.encoded_content)
        span.set_status(Status(StatusCode.OK))

    _call_in_trace_context(
        f=_log,
        span_name="artefact" if is_file else "named-value",
        traceparent=traceparent,
    )


def get_task_parameters(P: Dict[str, Any]):
    baggage = otel.baggage.get_all()

    _parameters_actor_name: Optional[str] = None
    if "_parameters_actor_name" in P and "_parameters_actor_name" in baggage:
        raise Exception(
            "Review logic. Parameter actor name found in both P and baggage"
        )
    elif "_parameters_actor_name" in P:
        _parameters_actor_name = str(P["_parameters_actor_name"])
    elif "_parameters_actor_name" in baggage:
        _parameters_actor_name = str(baggage["_parameters_actor_name"])
    else:
        # no parameter actor found, return default parameters
        return P

    # This has failed randomly in unit tests (1/2023) with "actor not found" from
    # notebooks.
    #
    #  - unit test works when run in isolation, but fails as part of whole test suite
    #  - the below monitors if retry would be a solution, but fails to catch this.
    parameter_actor = None
    e = None
    for _ in range(10):
        try:
            parameter_actor = ray.get_actor(
                _parameters_actor_name, namespace="pydar-ray-cluster"
            )
        except Exception as _e:
            e = _e
            time.sleep(0.1)

    if e is not None and parameter_actor is not None:
        raise Exception("ray.get_actor first failed and then worked after retry!")

    assert parameter_actor is not None
    return ray.get(parameter_actor.get.remote()).parameters


class ComposableLogsLogger:
    """
    Logger for writing artifacts/key-values as OpenTelemetry events
    """

    def __init__(self, P: Mapping[str, Any] = {}):
        assert isinstance(P, dict)

        # --- Check: should init of Ray cluster be done here?

        try:
            # - Connect to running Ray cluster if running
            # - Fail if no cluster running
            ray.init(
                address="auto",
                namespace="pydar-ray-cluster",
                ignore_reinit_error=True,
            )
        except:
            # No cluster running, start
            ray.init(namespace="pydar-ray-cluster")

        # ---
        parameters = get_task_parameters(P)

        # determine parent span where OpenTelemetry logs should be logged
        if "_opentelemetry_traceparent" in parameters:
            self._traceparent = parameters.get("_opentelemetry_traceparent")
            del parameters["_opentelemetry_traceparent"]
        else:
            self._traceparent = None

        self.parameters = {**P, **parameters}

    # --- log files ---

    def log_artefact(self, name: str, content: Union[bytes, str]):

        if isinstance(content, str):
            content_type = "utf-8"
        elif isinstance(content, bytes):
            content_type = "bytes"
        else:
            raise ValueError("Unknown content for artefact!")

        _log_named_value(
            name=name,
            content=content,
            content_type=content_type,
            is_file=True,
            traceparent=self._traceparent,
        )

    def log_figure(self, name: str, fig):
        """
        Log matplotlib figure as an png artefact
        """
        tmp_path = Path(tempfile.mkdtemp(prefix="pydar-temp", dir="/tmp/")) / "img.png"

        # plots are transparent by default.
        fig.savefig(tmp_path, facecolor="white", transparent=False)

        self.log_artefact(name=name, content=tmp_path.read_bytes())

        os.remove(tmp_path)

    # --- log values ---

    def log_value(self, name: str, value: Any):
        """
        Log generic json-serializiable value
        """
        _log_named_value(
            name=name, content=value, content_type="json", traceparent=self._traceparent
        )

    def log_string(self, name: str, value: str):
        if not isinstance(value, str):
            raise ValueError(f"log_string: value not a string {str(value)}")

        _log_named_value(
            name=name,
            content=value,
            content_type="string",
            traceparent=self._traceparent,
        )

    def log_int(self, name: str, value: int):
        if not isinstance(value, int):
            raise ValueError(f"log_int: value not an integer {str(value)}")

        _log_named_value(
            name=name, content=value, content_type="int", traceparent=self._traceparent
        )

    def log_boolean(self, name: str, value: bool):
        if not isinstance(value, bool):
            raise ValueError(f"log_boolean: value not an boolean {str(value)}")

        _log_named_value(
            name=name, content=value, content_type="bool", traceparent=self._traceparent
        )

    def log_float(self, name: str, value: float):
        if not isinstance(value, float):
            raise ValueError(f"log_float: value not a float {str(value)}")

        _log_named_value(
            name=name,
            content=value,
            content_type="float",
            traceparent=self._traceparent,
        )


TaskContext = ComposableLogsLogger


def get_task_context(P={}, use_mlflow_for_logging: bool = False) -> TaskContext:
    result = TaskContext(P=P)

    # note: we need to first set up connection to Ray (above) before we can set
    # up MLFlow server
    if use_mlflow_for_logging:
        # Note: We only do imports inside this function if "use_mlflow_for_logging"
        # is set. Otherwise we get a circular dependency since this function is also
        # used from the mlflow_server.server package.
        from composable_logs.mlflow_server.server import (
            configure_mlflow_connection_variables,
            ensure_mlflow_server_is_running,
        )

        ensure_mlflow_server_is_running()
        configure_mlflow_connection_variables(result._traceparent)

    return result
