import time

# -
import pytest

# -
from composable_logs.helpers import one, Failure
from composable_logs.opentelemetry_helpers import Spans, SpanRecorder
from composable_logs.opentelemetry_task_span_parser import parse_spans
from composable_logs.wrappers import task, run_dag

TASK_TIMEOUT_S = 0.5


@pytest.fixture(scope="module")
def spans() -> Spans:
    with SpanRecorder() as rec:

        @task(task_id="f-sleep-task", timeout_s=TASK_TIMEOUT_S)
        def f():
            time.sleep(1e6)

        assert run_dag(f()) == Failure(
            Exception("Timeout error: execution did not finish within timeout limit.")
        )

    assert len(rec.spans.exception_events()) == 1

    return rec.spans


def test__python_task__stuck_tasks__parse_spans(spans: Spans):
    workflow_summary = parse_spans(spans)
    assert workflow_summary.is_failure()

    for task_summary in [one(workflow_summary.task_runs)]:  # type: ignore
        assert task_summary.is_failure()
        assert "timeout" in str(task_summary.exceptions).lower()

        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        assert task_summary.attributes == {
            "task.id": "f-sleep-task",
            "task.type": "python",
            "task.num_cpus": 1,
            "task.timeout_s": TASK_TIMEOUT_S,
        }

        assert task_summary.timing.get_duration_s() > TASK_TIMEOUT_S

    assert len(workflow_summary.task_dependencies) == 0
