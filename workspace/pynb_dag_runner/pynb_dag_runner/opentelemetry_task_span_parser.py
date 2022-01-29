from typing import Any, Iterable, Tuple, Set, List

#
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId, get_duration_s


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

FlowDict = Any
TaskDict = Any
RunDict = Any
ArtefactDict = Any


def _artefact_iterator(spans: Spans, task_run_top_span) -> List[ArtefactDict]:
    return []


def _run_iterator(
    spans: Spans, task_top_span
) -> Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]:
    for task_run_top_span in spans.bound_under(task_top_span).filter(
        ["name"], "retry-call"
    ):
        run_dict = {
            "run_span_id": task_run_top_span["context"]["span_id"],
            "start_time": task_run_top_span["start_time"],
            "end_time": task_run_top_span["end_time"],
            "duration_s": get_duration_s(task_run_top_span),
            "status": task_run_top_span["status"],
            "run_attributes": (
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
            "task_span_id": task_top_span["context"]["span_id"],
            "start_time": task_top_span["start_time"],
            "end_time": task_top_span["end_time"],
            "duration_s": get_duration_s(task_top_span),
            "status": task_top_span["status"],
            "task_dict": (
                spans.bound_inclusive(task_top_span)
                #
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        yield task_dict, _run_iterator(spans, task_top_span)

    return iter([])


def get_pipeline_iterators(
    spans: Spans,
) -> Tuple[FlowDict, Iterable[Tuple[TaskDict, Iterable[RunDict]]]]:
    """
    Top level function that returns dict with pipeline scoped data and nested
    iterators for looping through tasks, runs, and artefacts logged to runs.

    Input is all OpenTelemetry spans logged for one pipeline run.
    """
    flow_attributes = {
        "task_dependencies": list(extract_task_dependencies(spans)),
        "flow_attributes": spans.get_attributes(allowed_prefixes={"flow."}),
    }

    return flow_attributes, _task_iterator(spans)
