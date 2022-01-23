from typing import Any, Tuple, Set, Optional, MutableMapping
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId
from pynb_dag_runner.core.dag_runner import AttributesDict


def get_all_attributes(
    spans: Spans, allowed_prefixes: Optional[Set[str]]
) -> AttributesDict:
    """
    Return union of all attributes in span collection `span`.

    Only consider only attribute keys that start with allowed prefixes listed in
    `allowed_prefixes`.

    Raise an exception if the span collection spans contains an attribute key with
    multiple distinct values in different spans.
    """
    result: MutableMapping[str, Any] = dict()

    def filter_attribute_dict(d: AttributesDict) -> AttributesDict:
        if allowed_prefixes is None:
            return d
        else:
            return {
                k: v
                for k, v in d.items()
                if any(k.startswith(prefix) for prefix in allowed_prefixes)
            }

    for span in spans:
        for k, v in filter_attribute_dict(span["attributes"]).items():
            if k in result:
                if result[k] != v:
                    raise ValueError(
                        f"Encountered key={k} with different values {result[k]} and {v}"
                    )
                # do nothing: {k: v} is already in result
            else:
                result[k] = v
    return result


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
