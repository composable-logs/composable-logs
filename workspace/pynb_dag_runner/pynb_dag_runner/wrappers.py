from typing import Any, TypeVar, List, Mapping, Union, Sequence, Optional
import inspect
import collections

# -
import pydantic as p

import ray
from ray.dag.function_node import FunctionNode
from ray import workflow

import opentelemetry as otel


# -
from pynb_dag_runner.opentelemetry_helpers import get_span_hexid, otel_add_baggage
from pynb_dag_runner.opentelemetry_task_span_parser import OpenTelemetrySpanId
from pynb_dag_runner.helpers import one, Try, Failure, Success
from pynb_dag_runner.tasks.task_opentelemetry_logging import ComposableLogsLogger

# --- schemas ---


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
    def __init__(self, exceptions: Sequence[Exception]):
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

    def __init__(self, parameters: Mapping[str, Any]):
        self.parameters = parameters

        # logging context determined by OpenTelemetry context propagation
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


def inject_task_context_argument_wrapper(f, parameters: Mapping[str, Any]):
    """
    TODO Revise logic.

     - Eg. inject only TaskContext for a variable that has this is type.
     - Ensure that there is at most one TaskContext argument in f

    """
    if "C" in inspect.signature(f).parameters.keys():
        extra = {"C": TaskContext(parameters=parameters)}

        def wrapped(*args, **kwargs):
            return f(*args, **extra, **kwargs)

        return wrapped

    else:
        return f


def timeout_guard_wrapper(f, timeout_s: Optional[float], num_cpus: int):
    """
    Return a wrapped function `f_wrapped(*args, **kwargs)` such that:
     - Return value a Try indicating either Success of Failure
     - A Failure is returned if:
        - the function f throws an Exception
        - or, execution of f takes more that `timeout_s`. In this case, the process
          running f is killed.
     - Otherwise a Success is returned with the function return value.
     - timeout_s == None indicates no timeout.
     - Executing is logged into two OpenTelemetry spans, and allocates `num_cpus`
       on the execution node in the Ray cluster.

    Notes:
     - we use a Ray actor since this can be kill (unlike ordinary Ray remote functions).

        https://docs.ray.io/en/latest/actors.html#terminating-actors

     - See also Ray issues/documentation re potential native Ray support for timeouts
        - "Support timeout option in Ray tasks"  https://github.com/ray-project/ray/issues/17451
        - "Set time-out on individual ray task" https://github.com/ray-project/ray/issues/15672

     - This function does not log the timeout_s parameter.

    """
    if not (timeout_s is None or timeout_s > 0):
        raise ValueError(
            f"timeout_guard_wrapper: timeout_s should be positive of None (no timeout), "
            f" not {timeout_s}."
        )

    @ray.remote(num_cpus=num_cpus)
    class ExecActor:
        def call(self, *args, **kwargs):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("call-python-function") as span:
                return (
                    Try.wrap(f)(*args, **kwargs)
                    # -
                    .log_outcome_to_opentelemetry_span(span, record_exception=True)
                )

    def make_call_with_timeout_guard(*args, **kwargs):
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("timeout-guard") as span:
            work_actor = ExecActor.remote()  # type: ignore
            future = work_actor.call.remote(*args, **kwargs)

            try:
                result = ray.get(future, timeout=timeout_s)

                # If python finished within timeout, do not log any Exception from f
                # into the timeout-guard span.
                result.log_outcome_to_opentelemetry_span(span, record_exception=False)

            except Exception as e:
                ray.kill(work_actor)

                result = Failure(
                    Exception(
                        "Timeout error: execution did not finish within timeout limit."
                    )
                )

                result.log_outcome_to_opentelemetry_span(span, record_exception=True)
            return result

    return make_call_with_timeout_guard


def task(
    task_id: str,
    task_parameters: Mapping[str, Any] = {},
    num_cpus: int = 1,
    timeout_s: Optional[float] = None,
):
    """
    Wrapper to convert Python function into Ray remote function that can be included
    in pynb-dag-runner Ray based DAG workflows.
    """
    if not (timeout_s is None or timeout_s > 0):
        raise ValueError("timeout_s should be positive of None (no timeout)")

    for k in task_parameters.keys():
        if not (k.startswith("task.") or k.startswith("workflow.")):
            raise ValueError(
                f"Task defined with task parameter {k}. "
                "Parameters should start with 'task.' or 'workflow."
            )

    def decorator(f):
        @ray.remote(
            retry_exceptions=False,
            num_cpus=0,
            max_retries=0,
        )
        def wrapped_f(*args: Try[TaskResult], **kwargs):
            # Short circuit this task if there are upstream errors.
            # In this case, exit with Failure containing an ExceptionGroup-error
            # collecting all upstream errors.
            upstream_exceptions = [arg.error for arg in args if not arg.is_success()]
            if len(upstream_exceptions) > 0:
                return Failure(flatten_exceptions(*upstream_exceptions))  # type: ignore

            args = [arg.get() for arg in args]  # type: ignore

            tracer = otel.trace.get_tracer(__name__)
            with tracer.start_as_current_span("execute-task") as span:
                this_task_span_id: str = get_span_hexid(span)

                # ---
                # Log all task parameters into new span.
                #
                # Note that when task is defined, we know the task parameters, but
                # we are not given the global workflow.* parameters. At task runtime,
                # these are determined by the span's baggage.
                augmented_task_parameters: Mapping[str, Any] = {
                    **otel.baggage.get_all(),
                    **task_parameters,
                    "task.task_id": task_id,
                    "task.num_cpus": num_cpus,
                    "task.timeout_s": -1 if timeout_s is None else timeout_s,
                }

                for k, v in augmented_task_parameters.items():
                    if v is None:
                        # Behavior of NULL OpenTelemetry attributes is undefined.
                        # See
                        # https://opentelemetry-python.readthedocs.io/en/latest/api/trace.span.html#opentelemetry.trace.span.Span.set_attributes
                        raise ValueError("OpenTelemetry attributes should be non-null")
                    span.set_attribute(k, v)

                # ---
                # Input arguments to this task are wrapped using TaskResult that
                # contain values and upstream OpenTelemetry span_id:s where these
                # were computed.
                #
                # We now know span_id of this task, so we can:
                #  - unwrap input values from upstream tasks
                #  - log task dependencies (parent_span_id, this_span_id)
                #
                # Of note: this is done dynamically at runtime.
                #
                # TODO: support also upsteam tasks passed in using kwargs
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

                del args

                # ---

                f_injected = inject_task_context_argument_wrapper(
                    f, augmented_task_parameters
                )

                f_timeout_guarded = timeout_guard_wrapper(
                    f_injected,
                    timeout_s=timeout_s,
                    num_cpus=num_cpus,
                )

                try_result: Try = (
                    f_timeout_guarded(*args_unwrapped, **kwargs)
                    # -
                    .log_outcome_to_opentelemetry_span(span, record_exception=False)
                )

            # Wrap any successful result into TaskResult
            return try_result.map_value(
                lambda x: TaskResult(result=x, span_id=this_task_span_id)
            )

        return wrapped_f.bind

    return decorator


def run_dag(
    dag: Union[FunctionNode, Sequence[FunctionNode]],
    workflow_parameters: Mapping[str, Any] = {},
) -> Try:
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
            #
            #  - if all nodes ran successfully: return a Try(Success) with return values
            #    of end nodes as a list.
            #
            #  - if any node(s) in the DAG fails: return a Try(Failure) with an
            #    ExceptionGroup collecting all exceptions. Note that two nodes in the
            #    DAG can fail in parallel.
            #
            # Notes:
            #
            #  - We do not want to [node.execute() for node in dag]. In the above DAG,
            #    this would run Node 1 three times. Rather, we start execution on a
            #    collect-node that waits for all upstream nodes.
            #
            #  - The collect node will not be seen in OpenTelemetry logs.
            #
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
                        f"Expected a Try[TaskResult] got {str(dag_result)[:100]}."
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
