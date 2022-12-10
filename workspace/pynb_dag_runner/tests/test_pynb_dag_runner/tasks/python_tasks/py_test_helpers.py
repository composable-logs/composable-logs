from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_helpers import get_duration_range_us, Spans


def get_time_range(spans: Spans, function_id: str, inner: bool):
    task_top_span = one(
        spans.filter(["name"], "execute-task")
        # -
        .filter(["attributes", "task.function_id"], function_id)
    )

    task_spans = spans.bound_under(task_top_span)

    inner_flag_to_span_dict = {
        # inner=True: return time range for span used for (inner) python
        # function call; this is where task cpu resources are reserved.
        True: one(task_spans.filter(["name"], "call-python-function")),
        # inner=False: return time range for top span of entire task
        False: task_top_span,
    }

    return get_duration_range_us(inner_flag_to_span_dict[inner])
