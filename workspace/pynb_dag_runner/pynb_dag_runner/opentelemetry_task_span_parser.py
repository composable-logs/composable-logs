from pathlib import Path
from typing import Any, Iterable, Tuple, Set, List

#
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId, get_duration_s
from pynb_dag_runner.notebooks_helpers import convert_ipynb_to_html


def extract_task_dependencies(spans: Spans) -> Set[Tuple[SpanId, SpanId]]:
    """
    From recorded Spans, extract any logged task dependencies as a set of from-to
    SpanID tuples.
    """
    return set(
        [
            (
                span["attributes"]["from_task_span_id"],
                span["attributes"]["to_task_span_id"],
            )
            for span in spans.filter(["name"], "task-dependency")
        ]
    )


# --- span parser ---

PipelineDict = Any
TaskDict = Any
RunDict = Any
ArtefactDict = Any


def _key_span_details(span):
    return {
        "span_id": span["context"]["span_id"],
        "start_time": span["start_time"],
        "end_time": span["end_time"],
        "duration_s": get_duration_s(span),
        "status": span["status"],
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
        result.append(
            {
                **_key_span_details(artefact_span),
                "name": artefact_span["attributes"]["name"],
                "encoding": "text/utf-8",
                "content": artefact_span["attributes"]["content"],
            }
        )

    return result


def add_html_notebook_artefacts(artefacts: List[ArtefactDict]) -> List[ArtefactDict]:
    """
    Helper function for iterating through a list of artefacts.

    The function returns the input list, but appended with html-artefact versions of
    any Jupyter notebook ipynb-artefacts (if present).
    """
    result = []

    for artefact_dict in artefacts:
        if (
            artefact_dict["name"].endswith(".ipynb")
            and artefact_dict["encoding"] == "text/utf-8"
        ):
            # convert evaluated .ipynb notebook into html page for easier viewing
            result.append(
                {
                    **artefact_dict,
                    **{
                        "name": str(Path(artefact_dict["name"]).with_suffix(".html")),
                        "encoding": "text/utf-8",
                        "content": convert_ipynb_to_html(artefact_dict["content"]),
                    },
                }
            )

        result.append(artefact_dict)
    return result


def _run_iterator(
    spans: Spans, task_top_span
) -> Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]:
    for task_run_top_span in spans.bound_under(task_top_span).filter(
        ["name"], "retry-call"
    ):
        run_dict = {
            **_key_span_details(task_run_top_span),
            "attributes": (
                spans.bound_inclusive(task_run_top_span)
                #
                .get_attributes(allowed_prefixes={"run."})
            ),
        }
        yield run_dict, _artefact_iterator(spans, task_run_top_span)

    return iter([])


def _task_iterator(spans: Spans) -> Iterable[Tuple[TaskDict, Iterable[RunDict]]]:
    for task_top_span in spans.filter(["name"], "execute-task"):
        task_dict = {
            **_key_span_details(task_top_span),
            "attributes": (
                spans.bound_inclusive(task_top_span)
                #
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        yield task_dict, _run_iterator(spans, task_top_span)

    return iter([])


def get_pipeline_iterators(
    spans: Spans,
) -> Tuple[PipelineDict, Iterable[Tuple[TaskDict, Iterable[RunDict]]]]:
    """
    Top level function that returns dict with pipeline scoped data and nested
    iterators for looping through tasks, runs, and artefacts logged to runs.

    Input is all OpenTelemetry spans logged for one pipeline run.
    """
    pipeline_attributes = {
        "task_dependencies": list(extract_task_dependencies(spans)),
        "attributes": spans.get_attributes(allowed_prefixes={"pipeline."}),
    }

    return pipeline_attributes, _task_iterator(spans)
