import uuid
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Union,
    Set,
    Tuple,
)

#
from pynb_dag_runner.helpers import del_key, dict_prefix_keys
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanDict,
    SpanId,
)
from pynb_dag_runner.notebooks_helpers import convert_ipynb_to_html
from pynb_dag_runner.tasks.task_opentelemetry_logging import SerializedData
from pynb_dag_runner.opentelemetry_helpers import (
    iso8601_range_to_epoch_us_range,
    iso8601_to_epoch_us,
)

# -
import pydantic as p


def extract_task_dependencies(spans: Spans) -> Set[Tuple[SpanId, SpanId]]:
    """
    From recorded Spans, extract any logged task dependencies as a set of from-to
    SpanID tuples.
    """
    return set(
        (
            span["attributes"]["from_task_span_id"],
            span["attributes"]["to_task_span_id"],
        )
        for span in spans.filter(["name"], "task-dependency")
    )


# --- span parser ---

PipelineDict = Mapping[str, Any]
TaskDict = Mapping[str, Any]
RunDict = Mapping[str, Any]
ArtifactDict = Mapping[str, Any]  # {name, type, content} in decoded form


def _decode_data_content_span(span: SpanDict):
    serialized_data = SerializedData(
        type=span["attributes"]["type"],
        encoding=span["attributes"]["encoding"],
        encoded_content=span["attributes"]["content_encoded"],
    )

    return {
        "name": span["attributes"]["name"],
        "type": serialized_data.type,
        "content": serialized_data.decode(),
    }


def add_html_notebook_artefacts(
    artefacts: Iterable[ArtifactDict],
) -> List[ArtifactDict]:
    """
    Helper function for iterating through a list of artefacts.

    The function returns the input list, but appended with html-artefact versions of
    any Jupyter notebook ipynb-artefacts (if present).
    """
    result: List[ArtifactDict] = []

    for artefact_dict in artefacts:
        if (
            artefact_dict["name"].endswith(".ipynb")
            and artefact_dict["type"] == "utf-8"
        ):
            # convert evaluated .ipynb notebook into html page for easier viewing
            result.append(
                {
                    **artefact_dict,
                    **{
                        "name": str(Path(artefact_dict["name"]).with_suffix(".html")),
                        "type": "utf-8",
                        "content": convert_ipynb_to_html(artefact_dict["content"]),
                    },
                }
            )

        result.append(artefact_dict)
    return result


# --- Data structure to represent: artifact data ---

ArtifactName = p.StrictStr


class ArtifactContent(p.BaseModel):
    name: p.StrictStr
    type: p.StrictStr
    content: Union[p.StrictStr, p.StrictBytes]

    def metadata_as_dict(self):
        return {
            "name": str(self.name),
            "type": str(self.type),
            "length": len(self.content),
        }

    @p.validator("type")
    def validate_type(cls, v):
        assert v in ["utf-8", "bytes"]
        return v

    def write(self, filepath: Path):
        if self.type == "utf-8":
            assert isinstance(self.content, str)
            filepath.write_text(self.content)
        elif self.type == "bytes":
            assert isinstance(self.content, bytes)
            filepath.write_bytes(self.content)
        else:
            raise ValueError("Internal error")


def _artefact_iterator(spans: Spans, task_run_top_span) -> Iterable[ArtifactContent]:
    for artefact_span in (
        spans.bound_under(task_run_top_span)
        # -
        .filter(["name"], "artefact")
        # -
        .filter(["status", "status_code"], "OK")
    ):
        artifact_dict = _decode_data_content_span(artefact_span)
        yield ArtifactContent(
            name=artifact_dict["name"],
            **del_key(artifact_dict, "name"),
        )

        if artifact_dict["name"] == "notebook.ipynb":
            assert artifact_dict["type"] == "utf-8"
            yield ArtifactContent(
                name=str(Path(artifact_dict["name"]).with_suffix(".html")),
                type="utf-8",
                content=convert_ipynb_to_html(artifact_dict["content"]),
            )


# --- Data structure to represent: logged values ---


LoggedValueName = p.StrictStr


class LoggedValueContent(p.BaseModel):
    type: p.StrictStr
    content: Any

    @p.validator("type")
    def validate_type(cls, v):
        assert v in ["utf-8", "bytes", "float", "bool", "json", "int"]
        return v

    def as_dict(self):
        return {"type": self.type, "value": self.content}


def _get_logged_named_values(
    spans: Spans, task_run_top_span
) -> Iterable[Tuple[LoggedValueName, LoggedValueContent]]:
    logged_values: List[str] = []

    for logged_value_span in (
        spans.bound_under(task_run_top_span)
        # -
        .filter(["name"], "named-value")
        # -
        .filter(["status", "status_code"], "OK")
    ):
        assert logged_value_span["attributes"].keys() == {
            "name",
            "type",
            "encoding",
            "content_encoded",
        }

        value_name: str = logged_value_span["attributes"]["name"]
        value_type: str = logged_value_span["attributes"]["type"]

        # Abort if same value has been logged multiple times.
        # (case eg for logging cost functions, not supported (yet))
        if value_name in logged_values:
            raise ValueError(
                f"Named value {value_name} has been logged multiple times."
            )
        logged_values.append(value_name)

        content = SerializedData(
            type=value_type,
            encoding=logged_value_span["attributes"]["encoding"],
            encoded_content=logged_value_span["attributes"]["content_encoded"],
        ).decode()

        yield (
            LoggedValueName(value_name),
            LoggedValueContent(type=value_type, content=content),
        )


AttributeKey = p.StrictStr
AttributeValues = Union[p.StrictInt, p.StrictFloat, p.StrictBool, p.StrictStr]
AttributeMapping = Mapping[AttributeKey, AttributeValues]

# --- Data structure to represent: task run summary ---


class Timing(p.BaseModel):
    start_time_iso8601: p.StrictStr
    end_time_iso8601: p.StrictStr

    # --- timing and task run related methods

    def get_start_time_epoch_us(self) -> int:
        return iso8601_to_epoch_us(self.start_time_iso8601)

    def get_end_time_epoch_us(self) -> int:
        return iso8601_to_epoch_us(self.end_time_iso8601)

    def get_duration_s(self) -> float:
        return round(
            (self.get_end_time_epoch_us() - self.get_start_time_epoch_us()) / 1e6, 3
        )

    def get_task_timestamp_range_us_epoch(self):
        """
        Return task execution timestamp range (as a range expressed in unix epoch us)
        """
        return iso8601_range_to_epoch_us_range(
            self.start_time_iso8601, self.end_time_iso8601
        )

    def as_dict(self):
        return {
            "start_iso8601": self.start_time_iso8601,
            "end_iso8601": self.end_time_iso8601,
            "duration_s": self.get_duration_s(),
        }


# Python 3.8 workaround
# https://github.com/pydantic/pydantic/issues/156
class NonEmptyString(p.ConstrainedStr):
    min_length = 1
    strict = True


OpenTelemetrySpanId = NonEmptyString


class TaskRunSummary(p.BaseModel):

    # The ID for OpenTelemetry top span for this task
    #  - generated by OpenTelemetry
    #  - format "0x0123456789abcdef", 64 bit.
    span_id: OpenTelemetrySpanId

    # The ID for the parent pipeline OpenTelemetry top span.
    parent_span_id: OpenTelemetrySpanId

    # eg "train-model", "evaluate-model", "ingest-data"
    task_id: NonEmptyString

    exceptions: List[Any]

    attributes: AttributeMapping

    timing: Timing

    # keep track of values/artifacts logged *during run time*
    logged_values: Dict[LoggedValueName, LoggedValueContent]
    logged_artifacts: List[ArtifactContent]

    # --- input validation
    @p.validator("span_id")
    def validate_otel_span_id(cls, v):
        if not v.startswith("0x"):
            raise ValueError(
                f"Tried to initialize OpenTelemetry span with id={v}. "
                "Expected id to start with 0x."
            )
        return v

    # ---
    def is_success(self) -> bool:
        return len(self.exceptions) == 0

    def is_failure(self) -> bool:
        return not self.is_success()

    # --- serialise into Python dict
    def as_dict(self):
        return {
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "task_id": self.task_id,
            **dict_prefix_keys("timing_", self.timing.as_dict()),
            #
            "is_success": self.is_success(),
            "exceptions": self.exceptions,
            #
            "attributes": self.attributes,
            #
            "logged_values": {k: v.as_dict() for k, v in self.logged_values.items()},
            "logged_artifacts": [
                artifact.metadata_as_dict() for artifact in self.logged_artifacts
            ],
        }


# --- Data structure to represent: pipeline (of multiple tasks) run summary ---


class PipelineSummary(p.BaseModel):
    span_id: OpenTelemetrySpanId

    timing: Timing

    # pipeline-level attributes
    attributes: AttributeMapping

    # summaries of all task runs than executed as part of pipeline
    task_runs: List[TaskRunSummary]

    task_dependencies: Set[Any]

    def is_success(self) -> bool:
        # Did all tasks run successfully?
        return all(task_run.is_success() for task_run in self.task_runs)

    def is_failure(self) -> bool:
        return not self.is_success()

    def as_dict(self):
        return {
            "span_id": self.span_id,
            **dict_prefix_keys("timing_", self.timing.as_dict()),
            "task_dependencies": list(self.task_dependencies),
            "attributes": self.attributes,
        }


def _task_run_iterator(
    top_span_id: str, pipeline_attributes: Mapping[str, Any], spans: Spans
) -> Iterable[TaskRunSummary]:
    for task_top_span in spans.filter(["name"], "execute-task").sort_by_start_time():
        task_attributes: Mapping[str, Any] = {
            **pipeline_attributes,  # inherited attributes from pipeline
            **(
                spans.bound_inclusive(task_top_span)
                # -
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        # TODO: task_id should be provided when creating a task.
        # For now determine task_id using the below heuristic
        task_id: str = "not-available"
        if "task.task_id" in task_attributes:
            task_id = task_attributes["task.task_id"]
        elif "task.notebook" in task_attributes:
            task_id = (
                task_attributes["task.notebook"]
                # -
                .replace(".py", "").split("/")[-1]
            )

        yield TaskRunSummary(
            span_id=task_top_span["context"]["span_id"],
            parent_span_id=top_span_id,
            task_id=task_id,
            timing=Timing(
                start_time_iso8601=task_top_span["start_time"],
                end_time_iso8601=task_top_span["end_time"],
            ),
            # was task run a success?
            exceptions=spans.bound_inclusive(task_top_span).exception_events(),
            # input parameters + logged data
            attributes=task_attributes,
            logged_values=dict(_get_logged_named_values(spans, task_top_span)),
            logged_artifacts=list(_artefact_iterator(spans, task_top_span)),
        )


def parse_spans(spans: Spans) -> PipelineSummary:
    """
    Parse spans into an easy to use object summarising outcomes of pipeline and
    individual tasks.

    Input is all OpenTelemetry spans logged for one pipeline run.
    """
    pipeline_attributes = spans.get_attributes(
        allowed_prefixes={
            # old API (to be removed)
            "pipeline.",
            # new Ray workflow-based API
            "workflow.",
        }
    )

    # TODO 1:
    # - potentially (top) span_id could also be passed into function as argument
    # - or, we could determine top node dynamically from input spans, provided it is unique
    #
    # TODO 2:
    # - Move to have a top span for pipeline. use that for ID and time-ranges.
    #   Currently we determine the time range dynamically for now, see below.
    if "pipeline.pipeline_run_id" in pipeline_attributes:
        top_span_id = pipeline_attributes["pipeline.pipeline_run_id"]
    else:
        top_span_id = "NO-TOP-SPAN--TEMP" + str(uuid.uuid4())

    return PipelineSummary(
        span_id=top_span_id,
        task_dependencies=extract_task_dependencies(spans),
        attributes=pipeline_attributes,
        # TODO: get time range from top span
        timing=Timing(
            start_time_iso8601=min(span["start_time"] for span in spans),
            end_time_iso8601=max(span["end_time"] for span in spans),
        ),
        task_runs=list(_task_run_iterator(top_span_id, pipeline_attributes, spans)),
    )
