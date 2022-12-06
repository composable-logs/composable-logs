from typing import List

#
from pynb_dag_runner.opentelemetry_helpers import SpanId, Spans
from pynb_dag_runner.helpers import one, pairs, flatten
from pynb_dag_runner.opentelemetry_helpers import get_duration_range_us, Spans


def assert_compatibility(spans: Spans, task_id_dependencies):
    """
    Test:
     - generic invariances for span timings (Steps 1, 2)
     - order constraints in dependency DAG are satisfied by output timings (Step 3)

    TODO
    This is not used currently. Delete or will this be needed (after refactor to
    Ray workflows)
    """

    # Step 1: all task-id:s in order dependencies must have at least one runlog
    # entry. (The converse need not hold.)
    top_spans: Spans = spans.filter(["name"], "invoke-task")
    task_ids_in_spans: List[str] = [span["attributes"]["task_id"] for span in top_spans]
    # each top span should have unique span_id
    assert len(set(task_ids_in_spans)) == len(task_ids_in_spans)

    task_ids_in_dependencies: List[str] = flatten(
        [[d["from"], d["to"]] for d in task_id_dependencies]
    )
    assert set(task_ids_in_dependencies) <= set(task_ids_in_spans)

    # Step 2: A task retry should not start before previous attempt for running task
    # has finished.
    for top_span in top_spans:
        task_id = top_span["attributes"]["task_id"]
        run_spans = list(
            spans.bound_under(top_span)
            .filter(["name"], "task-run")
            .sort_by_start_time()
        )
        assert len(run_spans) >= 1

        for retry_nr, run_span in enumerate(run_spans):
            assert run_span["attributes"]["task_id"] == task_id
            assert run_span["attributes"]["retry.nr"] == retry_nr

            if run_span["attributes"]["retry.max_retries"] > len(run_spans):
                assert list(run_spans)[-1]["status"] == {"status_code": "OK"}

        for s1, s2 in pairs(run_spans):
            assert get_duration_range_us(s1).stop < get_duration_range_us(s2).start

    # Step 3: Span durations should satisfy the same order constraints as in DAG
    # run order dependencies.
    for rule in task_id_dependencies:
        spans_from = spans.filter(["attributes", "task_id"], rule["from"])
        spans_to = spans.filter(["attributes", "task_id"], rule["to"])

        ts0 = max([get_duration_range_us(s).stop for s in spans_from])
        ts1 = min([get_duration_range_us(s).start for s in spans_to])
        assert ts0 < ts1


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
