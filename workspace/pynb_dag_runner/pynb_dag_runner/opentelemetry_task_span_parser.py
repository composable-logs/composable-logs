from typing import Dict, Tuple, Set
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId


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
