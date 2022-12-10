from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    MutableMapping,
    Sequence,
    Tuple,
    Set,
    List,
)

#
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanDict,
    SpanId,
    get_duration_s,
)
from pynb_dag_runner.notebooks_helpers import convert_ipynb_to_html
from pynb_dag_runner.tasks.task_opentelemetry_logging import SerializedData
from pynb_dag_runner.opentelemetry_helpers import iso8601_range_to_epoch_us_range

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
ArtefactDict = Mapping[str, Any]  # {name, type, content} in decoded form


def _key_span_details(span):
    return {
        "span_id": span["context"]["span_id"],
        "start_time": span["start_time"],
        "end_time": span["end_time"],
        "duration_s": get_duration_s(span),
        "status": span["status"],
    }


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


def _artefact_iterator(spans: Spans, task_run_top_span) -> List[ArtefactDict]:
    result = []
    for artefact_span in (
        spans.bound_under(task_run_top_span)
        # -
        .filter(["name"], "artefact")
        # -
        .filter(["status", "status_code"], "OK")
    ):
        result.append(_decode_data_content_span(artefact_span))

    return result


def add_html_notebook_artefacts(
    artefacts: Iterable[ArtefactDict],
) -> List[ArtefactDict]:
    """
    Helper function for iterating through a list of artefacts.

    The function returns the input list, but appended with html-artefact versions of
    any Jupyter notebook ipynb-artefacts (if present).
    """
    result: List[ArtefactDict] = []

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


def _get_logged_named_values(spans: Spans, task_run_top_span) -> Mapping[str, Any]:
    result: MutableMapping[str, Any] = {}

    for artefact_span in (
        spans.bound_under(task_run_top_span)
        # -
        .filter(["name"], "named-value")
        # -
        .filter(["status", "status_code"], "OK")
    ):
        assert artefact_span["attributes"].keys() == {
            "name",
            "type",
            "encoding",
            "content_encoded",
        }

        name: str = artefact_span["attributes"]["name"]

        if name in result:
            raise ValueError(f"Named value {name} has been logged multiple times.")

        serialized_data = SerializedData(
            type=artefact_span["attributes"]["type"],
            encoding=artefact_span["attributes"]["encoding"],
            encoded_content=artefact_span["attributes"]["content_encoded"],
        )

        result[name] = {"value": serialized_data.decode(), "type": serialized_data.type}

    return result


def _run_iterator(
    task_attributes: Mapping[str, Any], spans: Spans, task_top_span
) -> Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]:
    # --- deprecated ---
    for task_run_top_span in (
        spans.bound_under(task_top_span)
        .filter(["name"], "retry-call")
        .sort_by_start_time()  # TODO: sort by run.retry_nr instead
    ):
        # get all run attributes including attributes inherited from parent task
        # and pipeline.
        run_dict = {
            **_key_span_details(task_run_top_span),
            "attributes": {
                **task_attributes,
                **(
                    spans.bound_inclusive(task_run_top_span)
                    #
                    .get_attributes(allowed_prefixes={"run."})
                ),
            },
            "logged_values": _get_logged_named_values(spans, task_run_top_span),
        }
        yield run_dict, _artefact_iterator(spans, task_run_top_span)

    return iter([])


def _task_iterator(
    pipeline_attributes: Mapping[str, Any], spans: Spans
) -> Iterable[Tuple[TaskDict, Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]],]:
    # --- deprecated ---
    for task_top_span in spans.filter(["name"], "execute-task").sort_by_start_time():
        # get all task attributes including attributes inherited from pipeline
        task_attributes: Dict[str, Any] = {
            **pipeline_attributes,
            **(
                spans.bound_inclusive(task_top_span)
                # --
                .get_attributes(allowed_prefixes={"task."})
            ),
        }
        task_dict = {
            **_key_span_details(task_top_span),
            "attributes": task_attributes,
        }

        yield task_dict, _run_iterator(task_attributes, spans, task_top_span)

    return iter([])


# Deprecated: move to get_pipeline_task_artifact_iterators
def get_pipeline_iterators(
    spans: Spans,
) -> Tuple[
    PipelineDict,
    Iterable[Tuple[TaskDict, Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]]],
]:
    """
    Top level function that returns dict with pipeline scoped data and nested
    iterators for looping through tasks, runs, and artefacts logged to runs.

    Input is all OpenTelemetry spans logged for one pipeline run.
    """
    pipeline_attributes = spans.get_attributes(allowed_prefixes={"pipeline."})

    pipeline_dict = {
        "task_dependencies": list(extract_task_dependencies(spans)),
        "attributes": pipeline_attributes,
    }

    return pipeline_dict, _task_iterator(pipeline_attributes, spans)


# --- new stuff below ---


class PipelineSummary(p.BaseModel):
    task_dependencies: Set[Any]
    attributes: Mapping[str, Any]


class TaskRunSummary(p.BaseModel):
    span_id: str
    start_time_iso8601: str
    end_time_iso8601: str
    duration_s: float
    status: Any

    # -
    attributes: Mapping[str, Any]
    logged_values: Any

    @p.validator("span_id")
    def validate_otel_span_id(cls, v):
        if not v.startswith("0x"):
            raise ValueError(
                f"Tried to initialize OpenTelemetry span with id={v}. "
                "Expected id to start with 0x."
            )
        return v

    def time_range_epoch_us(self):
        return iso8601_range_to_epoch_us_range(
            self.start_time_iso8601, self.end_time_iso8601
        )


def _task_run_iterator(
    pipeline_attributes: Mapping[str, Any], spans: Spans
) -> Iterable[Tuple[TaskRunSummary, Sequence[ArtefactDict]]]:
    for task_top_span in spans.filter(["name"], "execute-task").sort_by_start_time():
        task_attributes: Dict[str, Any] = {
            **pipeline_attributes,  # inherited attributes from pipeline
            **(
                spans.bound_inclusive(task_top_span)
                # --
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        # determine status
        exceptions = spans.bound_inclusive(task_top_span).exception_events()
        if len(exceptions) == 0:
            status = {"status_code": "OK"}
        else:
            status = {"status_code": "ERROR", "exceptions": exceptions}

        task_run_summary = TaskRunSummary(
            span_id=task_top_span["context"]["span_id"],
            start_time_iso8601=task_top_span["start_time"],
            end_time_iso8601=task_top_span["end_time"],
            duration_s=get_duration_s(task_top_span),
            status=status,
            attributes=task_attributes,
            logged_values=list(_get_logged_named_values(spans, task_top_span)),
        )

        yield task_run_summary, list(_artefact_iterator(spans, task_top_span))


def get_pipeline_task_artifact_iterators(
    spans: Spans,
) -> Tuple[PipelineSummary, Sequence[Tuple[TaskRunSummary, Sequence[ArtefactDict]]]]:
    """
    --- New parser: this will replace `get_pipeline_iterators` ---

    Returns dict and iterator object to loop over tasks and task artefacts.

    Input is all OpenTelemetry spans logged for one pipeline run.

    Unpack return value as follows:

    ```
    pipeline_dict, task_run_it = get_pipeline_task_artifact_iterators(spans)
    # pipeline metadata in pipeline_dict

    # loop over tasks
    for task_run_dict, run_artifact_it in task_run_it:
        # task metadata in task_run_dict

        # loop over artifacts logged from task_run_dict
        for artifact_dict in run_artifact_it:
            # artifact metadata in artifact_dict
            ...
    ```

    Note: this replaces deprecated get_pipeline_iterators (and the new function
    does not allow for task retries).
    """
    pipeline_attributes = spans.get_attributes(allowed_prefixes={"pipeline."})

    pipeline_summary: PipelineSummary = PipelineSummary(
        task_dependencies=set(extract_task_dependencies(spans)),
        attributes=pipeline_attributes,
    )

    return pipeline_summary, list(_task_run_iterator(pipeline_attributes, spans))
