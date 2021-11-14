"""
Data types for representing tasks extracted from OpenTelemetry traces
emitted by pynb-dag-runner and Ray.
"""

from typing import List, Optional
import dataclasses
from dataclasses import dataclass

#
from pynb_dag_runner.tasks.tasks import RunParameters
from pynb_dag_runner.opentelemetry_helpers import Spans, Span
from pynb_dag_runner.opentelemetry_helpers import get_duration_s


class _To_Dict:
    def as_dict(self):
        return dataclasses.asdict(self)


@dataclass
class _LoggedSpan:
    span_id: str
    is_success: bool
    error: Optional[str]
    start_time: str
    end_time: str
    duration_s: float


@dataclass
class LoggedArtefact(_To_Dict):
    name: str
    content: str


@dataclass
class LoggedTaskRun(_LoggedSpan, _To_Dict):
    run_parameters: RunParameters
    artefacts: List[LoggedArtefact]


@dataclass
class LoggedTask(_LoggedSpan, _To_Dict):
    task_id: str
    task_parameters: RunParameters
    runs: List[LoggedTaskRun]


@dataclass
class LoggedJupytextTask(LoggedTask):
    task_type: str = "jupytext"


def _make_jupytext_logged_task(
    jupytext_span: Span, all_spans: Spans
) -> LoggedJupytextTask:
    is_success = jupytext_span["status"]["status_code"] == "OK"

    def make_run(run_span: Span):
        def make_artefact(artefact_span: Span) -> LoggedArtefact:
            return LoggedArtefact(
                name=artefact_span["attributes"]["name"],
                content=artefact_span["attributes"]["content"],
            )

        artefact_spans = all_spans.restrict_by_top(run_span).filter(
            ["name"], "artefact"
        )

        is_success = run_span["status"]["status_code"] == "OK"
        return LoggedTaskRun(
            span_id=run_span["context"]["span_id"],
            is_success=is_success,
            error=None if is_success else run_span["status"]["description"],
            start_time=run_span["start_time"],
            end_time=run_span["end_time"],
            duration_s=get_duration_s(run_span),
            run_parameters=run_span["attributes"],
            artefacts=[make_artefact(span) for span in artefact_spans],
        )

    run_spans = all_spans.restrict_by_top(jupytext_span).filter(["name"], "task-run")

    return LoggedJupytextTask(
        span_id=jupytext_span["context"]["span_id"],
        is_success=is_success,
        error=None if is_success else jupytext_span["status"]["description"],
        start_time=jupytext_span["start_time"],
        end_time=jupytext_span["end_time"],
        duration_s=get_duration_s(jupytext_span),
        task_id=jupytext_span["attributes"]["task_id"],
        task_parameters=jupytext_span["attributes"],
        runs=[make_run(span) for span in run_spans],
    )


def get_tasks(spans: Spans) -> List[LoggedTask]:
    """
    Convert a list of otel spans into a list of LoggedTask for easier processing.

    Notes:
     - this only outputs jupytext tasks.
     - jupytext tasks are assumed to not be nested.

    """
    jupytext_spans = spans.filter(["name"], "invoke-task").filter(
        ["attributes", "task_type"], "jupytext"
    )

    return [_make_jupytext_logged_task(jt_span, spans) for jt_span in jupytext_spans]
