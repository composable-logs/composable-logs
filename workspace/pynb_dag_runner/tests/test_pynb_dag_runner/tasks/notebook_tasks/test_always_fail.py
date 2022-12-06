from typing import Any, Dict

from pathlib import Path
from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.helpers import one
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder, read_key

from .nb_test_helpers import make_test_nb_task, TEST_NOTEBOOK_PATH


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_always_fail.py",
            max_nr_retries=1,
            parameters={"task.injected_parameter": 19238},
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__always_fail__validate_spans(spans: Spans):

    assert len(spans.exception_events()) > 0
    top_task_span = one(
        spans.filter(["name"], "execute-task")
        #
        .filter(["attributes", "task.task_type"], "jupytext")
    )
    assert top_task_span["status"] == {
        "status_code": "ERROR",
        "description": "Remote function call failed",
    }

    assert (
        read_key(top_task_span, ["attributes", "task.notebook"])
        == str((Path(__file__).parent))
        + "/jupytext_test_notebooks/notebook_always_fail.py"
    )

    top_retry_span = one(spans.filter(["name"], "retry-wrapper"))
    assert spans.contains_path(top_task_span, top_retry_span)
    assert read_key(top_retry_span, ["attributes", "task.max_nr_retries"]) == 1

    retry_span = one(spans.filter(["name"], "retry-call"))

    # there is one exception
    retry_spans: Spans = spans.bound_under(retry_span)
    assert len(retry_spans.exception_events()) == 1

    artefact_span = one(retry_spans.filter(["name"], "artefact"))
    assert "task.injected_parameter" in artefact_span["attributes"]["content_encoded"]

    spans.contains_path(top_task_span, top_retry_span, retry_span, artefact_span)
