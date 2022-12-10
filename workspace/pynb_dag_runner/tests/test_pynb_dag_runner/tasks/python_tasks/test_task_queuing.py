import time, datetime
import itertools as it
from functools import lru_cache

#
import pytest

#
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.helpers import range_intersection, range_is_empty
from pynb_dag_runner.core.dag_runner import (
    start_and_await_tasks,
    task_from_python_function,
)
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_task_artifact_iterators,
)

#
from pynb_dag_runner.opentelemetry_helpers import iso8601_range_to_epoch_us_range

# -
import opentelemetry as ot


@pytest.fixture(scope="module")
@lru_cache
def spans() -> Spans:
    def f_sleep(_):
        tracer = ot.trace.get_tracer(__name__)

        # Below we assert that function executions do not overlap and tasks are queued.
        #
        # For this, we need to distinguish between outer and inner timestamps for a
        # task:
        #
        # Outer timestamp range
        #    = start and end timestamps when task (wrapper) allocation starts
        #      (Ray CPU core allocation for these wrappers is 0)
        #
        # Inner timestamp range
        #    = start end timestamps when actual function is running
        #      (Ray CPU core allocation for this task = 1)
        #
        # Thus, outer tasks may overlap even if actual function executions have not been
        # started.
        #
        # TODO - replace with the pynb-dag-runner logger?
        with tracer.start_as_current_span("sleep-f-logger") as t1:
            t1.set_attribute(
                "task.inner_start_timestamp", datetime.datetime.now().isoformat()
            )
            time.sleep(0.5)
            t1.set_attribute(
                "task.inner_end_timestamp", datetime.datetime.now().isoformat()
            )

    with SpanRecorder() as rec:
        tasks = [
            task_from_python_function(
                f_sleep,
                attributes={"task.function_id": f"id#{function_id}"},
                timeout_s=10.0,
            )
            for function_id in range(4)
        ]

        start_ts = time.time_ns()
        _ = start_and_await_tasks(tasks, tasks, timeout_s=100, arg="dummy value")
        end_ts = time.time_ns()

        # Check 1: with only 2 CPU:s (reserved for unit tests, see ray.init call)
        # running the above tasks with no constraints should take > 1 secs.
        duration_ms = (end_ts - start_ts) // 1000000
        assert duration_ms >= 1000, duration_ms
    return rec.spans


def test__python_task__parallel_tasks_are_queued_based_on_available_ray_worker_cpus(
    spans: Spans,
):

    _, task_run_it = get_pipeline_task_artifact_iterators(spans)
    assert len(task_run_it) == 4

    task_runtime_ranges = []

    for task_run_summary, artefacts in task_run_it:  # type: ignore
        assert len(artefacts) == 0

        task_runtime_ranges.append(
            iso8601_range_to_epoch_us_range(
                task_run_summary.attributes["task.inner_start_timestamp"],
                task_run_summary.attributes["task.inner_end_timestamp"],
            )
        )

        assert task_run_summary.status == {"status_code": "OK"}

    # Check: since only 2 CPU:s are reserved (for unit tests, see above)
    # the intersection of three runtime ranges should always be empty.
    for r1, r2, r3 in it.combinations(task_runtime_ranges, 3):
        assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))
