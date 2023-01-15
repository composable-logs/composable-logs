import time, random


#
import ray
import pytest

#
from pynb_dag_runner.helpers import range_intersect, Try, Success, Failure
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
    SpanRecorder,
)
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans
from pynb_dag_runner.wrappers import task, run_dag, ExceptionGroup


@pytest.fixture(scope="module")
def spans_ok() -> Spans:
    @task(task_id=f"f-#1", num_cpus=1)
    def f1():
        time.sleep(1.0)

        return 1

    @task(task_id=f"f-#2", num_cpus=1)
    def f2():
        time.sleep(1.0)

        return 2

    with SpanRecorder() as rec:
        assert Success([1, 2]) == run_dag(dag=[f1(), f2()])

    return rec.spans


def test__python_task__parallel_tasks__success(spans_ok: Spans):
    pipeline_summary = parse_spans(spans_ok)

    # check attributes
    ids = []

    ranges = []
    for task_summary in pipeline_summary.task_runs:  # type: ignore
        assert task_summary.is_success()
        assert len(task_summary.logged_artifacts) == 0
        assert len(task_summary.logged_values) == 0

        assert task_summary.task_id == task_summary.attributes["task.task_id"]
        ids.append(task_summary.attributes["task.task_id"])
        ranges.append(task_summary.timing.get_task_timestamp_range_us_epoch())

    # Check: since there are no order constraints, the time ranges should
    # overlap provided tests are run on 2+ CPU cores
    assert set(ids) == {"f-#1", "f-#2"}
    assert range_intersect(*ranges)


# --- tests error handling if one of parallel tasks fail ---


@pytest.fixture(scope="module")
def spans_fail() -> Spans:
    @task(task_id="par-task-f", num_cpus=0)
    def f():
        time.sleep(0.5)

    @task(task_id="par-task-g", num_cpus=0)
    def g():
        raise Exception("task-g failure")

    @task(task_id="par-task-h", num_cpus=0)
    def h():
        time.sleep(0.5)

    with SpanRecorder() as rec:
        result = run_dag(dag=[f(), g(), h()])

    assert result == Failure(Exception("task-g failure"))
    return rec.spans


def test__python_task__parallel_tasks__fail(spans_fail: Spans):
    pipeline_summary = parse_spans(spans_fail)

    assert pipeline_summary.attributes == {}

    assert len(pipeline_summary.task_runs) == 3

    for task_summary in pipeline_summary.task_runs:  # type: ignore
        if task_summary.task_id in ["par-task-f", "par-task-h"]:
            assert task_summary.is_success()
        elif task_summary.task_id == "par-task-g":
            assert not task_summary.is_success()
            assert len(task_summary.exceptions) == 1
            assert "task-g failure" in str(task_summary.exceptions)
        else:
            raise Exception(f"Unknown task-id={task_summary.task_id}")

    assert len(pipeline_summary.task_dependencies) == 0


# --- 5 node test dag ---


def get_5dag_spans(*node_result: Try):
    """

    Return Spans after running the below DAG where each Node outputs
    re

    Node 0  -----+---> Node 2 ---+----> Node 3
                /                 \
    Node 1  ---/                   ---> Node 4

    We use this setup to test error handling.

    """

    def task_f(node_id: int, output: Try):
        @task(task_id=f"node-{node_id}", num_cpus=0)
        def f(*args):
            time.sleep(random.random())

            args_str = ";".join([str(x) for x in args])

            if output.is_success():
                return f"[{node_id}: ({args_str}) -> {output.get()}]"
            else:
                raise output.error  # type: ignore

        return f

    f0, f1, f2, f3, f4 = [task_f(*xs) for xs in enumerate(node_result)]

    # set up DAG
    node2 = f2(f0(), f1())
    node3 = f3(node2)
    node4 = f4(node2)
    dag = [node3, node4]

    with SpanRecorder() as rec:
        result = run_dag(dag=dag)

    return result, rec.spans


def test_5dag_outputs_with_no_errors():
    inputs = [Success(i) for i in "ABCDE"]

    result, spans = get_5dag_spans(*inputs)

    assert (
        Success(
            [
                "[3: ([2: ([0: () -> A];[1: () -> B]) -> C]) -> D]",
                "[4: ([2: ([0: () -> A];[1: () -> B]) -> C]) -> E]",
            ]
        )
        == result
    )

    # --- check spans
    pipeline_summary = parse_spans(spans)

    assert len(pipeline_summary.task_runs) == 5
    assert pipeline_summary.is_success()
    assert len(pipeline_summary.task_dependencies) == 4


def test_5dag_when_first_node_fails():
    inputs = [Failure(Exception("A-failed"))] + [Success(i) for i in "BCDE"]

    result, spans = get_5dag_spans(*inputs)

    # --- check that exception is returned (only once, even though exceptions are
    # propagated to both end nodes)
    expected_result = Failure(Exception("A-failed"))
    assert expected_result == result

    # --- check spans
    pipeline_summary = parse_spans(spans)

    # Nodes 0 and 1 are executed before computation is short-cirtuited, they sould
    # be in logs
    assert len(pipeline_summary.task_runs) == 2
    assert pipeline_summary.is_failure()
    assert len(pipeline_summary.task_dependencies) == 0


def test_5dag_when_first_two_nodes_fail():
    inputs = (
        [Failure(Exception("A failed"))]
        + [Failure(Exception("B failed"))]
        + [Success(i) for i in "CDE"]
    )

    result, spans = get_5dag_spans(*inputs)

    expected_result = Failure(
        ExceptionGroup([Exception("A failed"), Exception("B failed")])
    )
    assert expected_result == result

    # --- check spans
    pipeline_summary = parse_spans(spans)

    assert len(pipeline_summary.task_runs) == 2
    assert pipeline_summary.is_failure()
    assert len(pipeline_summary.task_dependencies) == 0


def test_python_task_input_node_may_execute_once_or_separately_for_each_downstream_node():
    """
    Depending on how the DAG is set up, Node 0 should only be executed once or twice.

    Node 0 ---+----> Node 1
               \
                ---> Node 2

    """

    @ray.remote
    class CallCounter:
        def __init__(self):
            self.count = 0

        def get_count(self):
            self.count += 1
            return self.count

    call_counter = CallCounter.remote()  # type: ignore

    @task(task_id="node0")
    def node0():
        return ray.get(call_counter.get_count.remote())

    @task(task_id="node1")
    def node1(arg):
        return f"1:{arg}"

    @task(task_id="node2")
    def node2(arg):
        return f"2:{arg}"

    # test dag where node0 is executed once
    n0 = node0()
    assert run_dag(dag=[node1(n0), node2(n0)]) == Success(["1:1", "2:1"])

    # test alternative dag, where counter is increased each time node0 is executed
    assert run_dag(dag=[node1(node0()), node2(node0())]) in [
        Success(["1:2", "2:3"]),
        Success(["1:3", "2:2"]),
    ]
