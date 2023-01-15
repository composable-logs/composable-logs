from typing import Any, Dict, TypeVar, List, Union, Sequence
import inspect
import collections

# -
import pydantic as p

import ray
from ray.dag.function_node import FunctionNode
from ray import workflow

import opentelemetry as otel
from opentelemetry.trace import StatusCode, Status  # type: ignore


# -
from pynb_dag_runner.opentelemetry_helpers import get_span_hexid, otel_add_baggage
from pynb_dag_runner.opentelemetry_task_span_parser import OpenTelemetrySpanId
from pynb_dag_runner.helpers import one, Try, Failure, Success
from pynb_dag_runner.tasks.task_opentelemetry_logging import ComposableLogsLogger

# --- schemas ---

A = TypeVar("A")


class TaskResult(p.BaseModel):
    """
    Wrapper used to pass arguments to downstream tasks.

    This wrapper allows us to keep track of the depdendencies between tasks; eg., when
    Task 4 below starts, we can log dependencies 1->4, 2->4, 3->4 (and identify a task
    with its top SpanID).

    task 1 ---\
               \
    task 2 -----+---> task 4
               /
    task 3 ---/

    """

    result: Any

    span_id: OpenTelemetrySpanId

    def __str__(self):
        return f"TaskResult(result={self.result}, span_id={self.span_id})"


# ---


class ExceptionGroup(Exception):
    # Exception subclass to contain ordered list of one or more Exceptions
    #
    # When creating an ExceptionGroup, duplicate exceptions are removed (where two
    # exceptions are equal if their string-representations coincide).
    #
    # --
    # An ExceptionGroup implementation would be available in Python 3.11, or in the
    # anyio-library (available as dependency of Ray, but this does not seem to inherit
    # from Exception?)
    def __init__(self, exceptions: List[Exception]):
        self.exceptions = []

        # add listed exceptions in given order, with duplicates removed
        str_exceptions_added = []
        for exception in exceptions:
            if str(exception) not in str_exceptions_added:
                self.exceptions.append(exception)
                str_exceptions_added.append(str(exception))

    def __str__(self):
        # Note: str(ExceptionGroup([e])) == str(e)
        return "-------\n".join(str(e) for e in self.exceptions)

    def __eq__(self, other):
        if isinstance(other, ExceptionGroup):
            if len(self.exceptions) != len(other.exceptions):
                return False
            else:
                return all(
                    str(x) == str(y) for x, y in zip(self.exceptions, other.exceptions)
                )
        else:
            return False


def flatten_exceptions(
    *exceptions: Union[Exception, ExceptionGroup]
) -> Union[Exception, ExceptionGroup]:
    assert len(exceptions) > 0
    unwrapped_exceptions = []

    for exception in exceptions:
        if isinstance(exception, ExceptionGroup):
            unwrapped_exceptions += exception.exceptions
        else:
            unwrapped_exceptions.append(exception)

    return (
        ExceptionGroup(unwrapped_exceptions)
        if len(unwrapped_exceptions) > 1
        else one(unwrapped_exceptions)
    )


class TaskContext:
    """
    TaskContext

    Same lifetime as the Python task (ie. task context has same lifetime as the task
    OpenTelemetry span).
    """

    def __init__(self, span, parameters: Dict[str, Any]):
        self.span = span
        self.parameters = parameters

        self.logger = ComposableLogsLogger(parameters)

    # --- forward log-methods to ComposableLogsLogger

    def log_figure(self, name: str, fig):
        self.logger.log_figure(name, fig)

    def log_artefact(self, name: str, content: Union[bytes, str]):
        self.logger.log_artefact(name, content)

    def log_value(self, name: str, value: Any):
        self.logger.log_value(name, value)

    def log_string(self, name: str, value: str):
        self.logger.log_string(name, value)

    def log_int(self, name: str, value: int):
        self.logger.log_int(name, value)

    def log_boolean(self, name: str, value: bool):
        self.logger.log_boolean(name, value)

    def log_float(self, name: str, value: float):
        self.logger.log_float(name, value)


def task(task_id: str, task_parameters: Dict[str, Any] = {}, num_cpus: int = 1):
    """
    Wrapper to convert Python function into Ray remote function that can be included
    in pynb-dag-runner Ray based DAG workflows.
    """

    def decorator(f):
        @workflow.options(task_id=task_id)  # type: ignore
        @ray.remote(retry_exceptions=False, num_cpus=num_cpus, max_retries=0)
        def wrapped_f(*args: Try[TaskResult], **kwargs):
            # Short circuit this task if there are upstream errors.
            # In this case, exit with Failure containing an ExceptionGroup-error
            # collecting all upstream errors.
            upstream_exceptions = [arg.error for arg in args if not arg.is_success()]
            if len(upstream_exceptions) > 0:
                return Failure(flatten_exceptions(*upstream_exceptions))  # type: ignore

            args = [arg.get() for arg in args]  # type: ignore
            # -

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

                try_result = Try.call(lambda: f(*args_unwrapped, **extra, **kwargs))

                if try_result.is_success():
                    span.set_status(Status(StatusCode.OK))
                else:
                    span.record_exception(try_result.error)
                    span.set_status(Status(StatusCode.ERROR, "Failure"))

            # Wrap result into TaskResult
            return try_result.map_value(
                lambda x: TaskResult(result=x, span_id=this_task_span_id)
            )

        return wrapped_f.bind

    return decorator


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
            return run_dag([dag]).map_value(lambda xs: one(xs))

        elif isinstance(dag, collections.Sequence):
            # Execute DAG with multiple ends like:
            #
            #          ---> Node 2
            #         /
            #  Node 1 ----> Node 3
            #         \
            #          ---> Node 4
            #
            # Now dag_run([N2, N3, N4]) awaits the results for all end nodes,
            # and returns a Try that:
            #  - if all nodes ran successfully: return a Try(Success) with return values
            #    of end nodes as a list.
            #
            #  - if any node(s) in the DAG fails: return a Try(Failure) with an
            #    ExceptionGroup collecting all exceptions. Note that two nodes in the
            #    DAG can fail in parallel.
            #
            # Note:
            #  - We do not want to [node.execute() for node in dag]. In the above DAG,
            #    this would run Node 1 three times. Rather, we start execution on a
            #    collect-node that waits for all upstream nodes.
            #
            #  - The collect node will not be seen in OpenTelemetry logs.

            @workflow.options(task_id="collect-nodes")  # type: ignore
            @ray.remote(retry_exceptions=False, num_cpus=0, max_retries=0)
            def collect(*args):
                return args

            dag_results = ray.get(collect.bind(*dag).execute())  # type: ignore

            # verify type of return values
            for dag_result in dag_results:
                if not (
                    isinstance(dag_result, Try)
                    and (
                        dag_result.is_failure()
                        or isinstance(dag_result.value, TaskResult)
                    )
                ):
                    raise Exception(
                        f"Expected a Try[TaskResult] got {str(dag_result)[:100]}"
                    )

            dag_errors: List[Exception] = [
                result.error for result in dag_results if result.is_failure()
            ]

            if len(dag_errors) > 0:
                return Failure(flatten_exceptions(*dag_errors))
            else:
                return Success([dag_result.get().result for dag_result in dag_results])

        else:
            raise Exception(f"Unknown input to run_dag {type(dag)}")
