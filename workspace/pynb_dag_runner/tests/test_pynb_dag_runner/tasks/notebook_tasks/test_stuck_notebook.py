import pytest

#
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
    get_duration_s,
)

from .nb_test_helpers import make_test_nb_task

# Currently, timeout canceling is done on Ray level, but error handling and
# recovery is done only within the Python process (using try .. catch).
#
# Therefore, timeout-canceled tasks can not currently do proper error handling.
# Eg., there would be no notebook artifact logged from a timeout-canceled task


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_stuck.py",
            max_nr_retries=1,
            timeout_s=10.0,
            parameters={},
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__stuck_notebook__validate_spans(spans: Spans):

    top_task_span = one(
        spans.filter(["name"], "execute-task")
        #
        .filter(["attributes", "task.task_type"], "jupytext")
    )

    assert top_task_span["status"] == {
        "description": "Remote function call failed",
        "status_code": "ERROR",
    }

    timeout_guard_span = one(spans.filter(["name"], "timeout-guard"))
    assert timeout_guard_span["status"] == {
        "status_code": "ERROR",
        "description": "Timeout",
    }

    spans.contains_path(top_task_span, timeout_guard_span)

    assert get_duration_s(top_task_span) > get_duration_s(timeout_guard_span) > 10.0

    assert len(spans.exception_events()) == 1

    # notebook evaluation never finishes, and is cancled by Ray. Therefore no
    # artefact ipynb content is logged
    assert len(spans.filter(["name"], "artefact")) == 0
