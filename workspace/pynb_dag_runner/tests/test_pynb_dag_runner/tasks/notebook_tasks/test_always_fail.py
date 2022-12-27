from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.helpers import del_key, one
from pynb_dag_runner.core.dag_runner import start_and_await_tasks
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanRecorder
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans

#
from .nb_test_helpers import make_test_nb_task, TEST_NOTEBOOK_PATH

TEST_TASK_PARAMETERS = {"task.injected_parameter": 123123}


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    with SpanRecorder() as rec:
        jupytext_task = make_test_nb_task(
            nb_name="notebook_always_fail.py",
            max_nr_retries=1,
            parameters=TEST_TASK_PARAMETERS,
        )
        _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

    return rec.spans


def test__jupytext__always_fail__parse_spans(spans: Spans):
    pipeline_summary = parse_spans(spans)

    # assert there is one task
    task_summary = one(pipeline_summary.task_runs)

    # assert that exception is logged
    assert not task_summary.is_success()
    assert len(task_summary.exceptions) == 2
    assert "This notebook always fails" in str(task_summary.exceptions)

    # asset attributes are logged
    # (str needed to avoid mypy validation error)
    assert "notebook_always_fail.py" in str(task_summary.attributes["task.notebook"])

    assert del_key(task_summary.attributes, "task.notebook") == {
        **TEST_TASK_PARAMETERS,
        "task.max_nr_retries": 1,
        "task.num_cpus": 1,
        "task.task_type": "jupytext",
        "task.timeout_s": 10.0,
    }
