from typing import Any, Dict
import inspect

# -
import ray

from ray import workflow
import opentelemetry as otel


# -
from pynb_dag_runner.opentelemetry_helpers import get_span_hexid, otel_add_baggage
import pydantic as p
from pynb_dag_runner.opentelemetry_task_span_parser import OpenTelemetrySpanId

# --- schemas ---


class TaskResult(p.BaseModel):
    """
    Wrapper used to pass arguments to downstream tasks.

    This wrapper allows us to keep track of the depdendencies between tasks; eg., when
    Task 4 below starts, we can log dependencies 1->4, 2->4, 3->4 (and identify a task
    with its top SpanID).

    task 1 ---\
               \
    task 2 -------> task 4
               /
    task 3 ---/

    """

    result: Any

    span_id: OpenTelemetrySpanId


# ---


class TaskContext:
    """
    TaskContext

    Same lifetime as the Python task (ie. task context has same lifetime as the task
    OpenTelemetry span).
    """

    def __init__(self, span, parameters: Dict[str, Any]):
        self.span = span
        self.parameters = parameters


def task(task_id: str, task_parameters: Dict[str, Any] = {}, num_cpus: int = 1):
    """
    Wrapper to convert Python function into Ray remote function that can be included
    in pynb-dag-runner Ray based DAG workflows.
    """

    def decorator(f):
        @workflow.options(task_id=task_id)  # type: ignore
        @ray.remote(retry_exceptions=False, num_cpus=num_cpus, max_retries=0)
        def wrapped_f(*args, **kwargs):
            tracer = otel.trace.get_tracer(__name__)

            with tracer.start_as_current_span("execute-task") as span:
                this_task_span_id: str = get_span_hexid(span)

                # --- unwrap values from upstream tasks and log task dependencies
                args_unwrapped = []
                for arg in args:
                    if isinstance(arg, TaskResult):
                        args_unwrapped.append(arg.result)
                    else:
                        args_unwrapped.append(arg)

                for arg in args:
                    if isinstance(arg, TaskResult):
                        with tracer.start_as_current_span("task-dependency") as subspan:
                            subspan.set_attribute("from_task_span_id", arg.span_id)
                            subspan.set_attribute("to_task_span_id", this_task_span_id)

                for _, v in kwargs.items():
                    if isinstance(v, TaskResult):
                        raise Exception(
                            "task composition not yet supported using kwargs"
                        )

                # ---

                # --- log attributes to this task-span
                augmented_task_parameters: Dict[str, Any] = {
                    "task.task_id": task_id,
                    "task.num_cpus": num_cpus,
                    **task_parameters,
                }

                for k, v in augmented_task_parameters.items():
                    span.set_attribute(k, v)
                    otel_add_baggage(k, v)

                # Inject logger if needed
                if "C" in inspect.signature(f).parameters.keys():
                    extra = {
                        "C": TaskContext(
                            span,
                            parameters={
                                **augmented_task_parameters,
                                # global (workflow-level) parameters are stored
                                # as OpenTelemetry baggage
                                **otel.baggage.get_all(),
                            },
                        )
                    }
                else:
                    extra = {}

                # Note that any exception thrown here seems to be logged multiple times
                # by Ray/OpenTelemetry
                result = f(*args_unwrapped, **extra, **kwargs)

                return TaskResult(
                    result=result,
                    span_id=this_task_span_id,
                )

        return wrapped_f.bind

    return decorator


from ray.dag.function_node import FunctionNode
from typing import Union, Sequence
import collections


def run_dag(
    dag: Union[FunctionNode, Sequence[FunctionNode]],
    workflow_parameters: Dict[str, Any] = {},
):
    """
    Run a Ray DAG with OpenTelemetry logging
    """
    tracer = otel.trace.get_tracer(__name__)

    with tracer.start_as_current_span("dag-top-span") as span:
        # ensure global parameters are logged and can be accessed from subspans
        for k, v in workflow_parameters.items():
            span.set_attribute(k, v)
            otel_add_baggage(k, v)

        if isinstance(dag, FunctionNode):
            dag_result = ray.get(dag.execute())  # type: ignore
            assert isinstance(dag_result, TaskResult)
            return dag_result.result

        elif isinstance(dag, collections.Sequence):
            # Execute DAG with multiple ends like:
            #
            #       --> N2
            #      /
            #  N1 ----> N3
            #      \
            #       --> N4
            #
            # Now dag_run([N2, N3, N4]) allows us to await results for all end nodes.
            #
            # Returns output of end nodes as a list.
            dag_results = ray.get([node.execute() for node in dag])  # type: ignore

            for dag_result in dag_results:
                assert isinstance(dag_result, TaskResult)

            return [dag_result.result for dag_result in dag_results]

        else:
            raise Exception(f"Unknown input to run_dag {type(dag)}")
