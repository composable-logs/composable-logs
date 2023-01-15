import time, datetime
import itertools as it

#
import pytest
import opentelemetry as ot

#
from pynb_dag_runner.helpers import range_intersection, range_is_empty
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
    iso8601_range_to_epoch_us_range,
)
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans

#
from pynb_dag_runner.wrappers import task, run_dag


@pytest.fixture(scope="module")
def spans() -> Spans:
    # Below we assert that function executions do not overlap and tasks are queued
    # taking into account num_cpus for each task.
    #
    # For this, we distinguish between outer and inner timestamps for task:
    #
    # Outer timestamp range
    #    = start and end timestamps when task (wrapper) allocation starts
    #      (Ray CPU core allocation for these wrappers is 0)
    #
    # Inner timestamp range
    #    = start end timestamps when actual function is running
    #      (Ray CPU core allocation for this task = 1)
    #
    # Thus, outer tasks may potentially overlap even even if the actual function
    # execution has not been started.
    #
    # TODO
    #  - is this still correct with new Ray workflow based execution?
    #  - replace with the pynb-dag-runner logger?

    def f():
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("sleep-f-logger") as t1:
            t1.set_attribute(
                "task.inner_start_timestamp", datetime.datetime.now().isoformat()
            )
            time.sleep(0.5)
            t1.set_attribute(
                "task.inner_end_timestamp", datetime.datetime.now().isoformat()
            )

    with SpanRecorder() as rec:
        run_dag(dag=[task(task_id=f"task-{k}", num_cpus=1)(f)() for k in range(4)])

    return rec.spans


def test__python_task__parallel_tasks_are_queued_based_on_available_ray_worker_cpus(
    spans: Spans,
):

    pipeline_summary = parse_spans(spans)
    assert len(pipeline_summary.task_runs) == 4

    task_runtime_ranges = []

    for task_summary in pipeline_summary.task_runs:  # type: ignore
        assert task_summary.is_success()
        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        task_runtime_ranges.append(
            iso8601_range_to_epoch_us_range(
                task_summary.attributes["task.inner_start_timestamp"],
                task_summary.attributes["task.inner_end_timestamp"],
            )
        )

    # Check 1: with only 2 CPU:s (reserved for unit tests, see ray.init call)
    # running the above tasks with no constraints should take > 1 secs.
    assert pipeline_summary.timing.get_duration_s() > 1.0

    # Check 2: since only 2 CPU:s are reserved (for unit tests, see above)
    # the intersection of three runtime ranges should always be empty.
    for r1, r2, r3 in it.combinations(task_runtime_ranges, 3):
        assert range_is_empty(range_intersection(r1, range_intersection(r2, r3)))
