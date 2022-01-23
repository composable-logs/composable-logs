import asyncio
from dataclasses import dataclass
from typing import (
    Awaitable,
    List,
    Optional,
    TypeVar,
    Generic,
    Callable,
    Protocol,
)

#
import ray
import opentelemetry as otel
from opentelemetry.trace.span import Span
from opentelemetry.trace import StatusCode, Status  # type: ignore

#
from pynb_dag_runner.helpers import pairs, Try
from pynb_dag_runner.ray_helpers import try_f_with_timeout_guard
from pynb_dag_runner.ray_helpers import (
    RayMypy,
    try_f_with_timeout_guard,
    retry_wrapper_ot,
)
from pynb_dag_runner.ray_mypy_helpers import RemoteGetFunction, RemoteSetFunction
from pynb_dag_runner.opentelemetry_helpers import SpanId, get_span_hexid, AttributesDict


# -- types --

A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")
U = TypeVar("U")
V = TypeVar("V")
W = TypeVar("W")


@dataclass(frozen=True, eq=True)
class TaskOutcome(Generic[A]):
    span_id: SpanId
    return_value: Optional[A]
    error: Optional[BaseException]


X = TypeVar("X", contravariant=True)
Y = TypeVar("Y", covariant=True)


class RemoteTaskP(Protocol[X, Y]):
    """
    Protocol to encode methods by remote Ray GenTask_OT-Actor
    """

    @property
    def start(self) -> RemoteSetFunction[X]:
        ...

    @property
    def add_callback(self) -> RemoteSetFunction[Callable[[Y], Awaitable[None]]]:
        ...

    @property
    def get_task_result(self) -> RemoteGetFunction[Y]:
        ...

    @property
    def get_span_id(self) -> RemoteGetFunction[str]:
        ...

    @property
    def has_started(self) -> RemoteGetFunction[bool]:
        ...

    @property
    def has_completed(self) -> RemoteGetFunction[bool]:
        ...


@ray.remote(num_cpus=0)
class GenTask_OT(Generic[U, A, B], RayMypy):
    """
    Represent a task that a can be run once
    """

    def __init__(
        self,
        f_remote: Callable[[U], Awaitable[A]],
        combiner: Callable[[Span, Try[A]], B],
        on_complete_callbacks: List[Callable[[B], Awaitable[None]]] = [],
        attributes: AttributesDict = {},
    ):
        def create_future():
            return asyncio.get_running_loop().create_future()

        self._f_remote: Callable[[U], Awaitable[A]] = f_remote
        self._combiner: Callable[[Span, Try[A]], B] = combiner
        self._on_complete_callbacks: List[
            Callable[[B], Awaitable[None]]
        ] = on_complete_callbacks
        self._attributes: AttributesDict = attributes
        self._start_called = False
        self._future_span_id: asyncio.Future[SpanId] = create_future()
        self._future_result: asyncio.Future[B] = create_future()

    def add_callback(self, cb: Callable[[B], Awaitable[None]]) -> None:
        """
        Note:
        - Method can only be called before first call to start-method.
        """
        if self.has_started():
            raise Exception("Cannot add callbacks once task has started")

        self._on_complete_callbacks.append(cb)

    def _set_result(self, value: B):
        self._future_result.set_result(value)  # type: ignore

    def _set_span_id(self, span: Span):
        self._future_span_id.set_result(get_span_hexid(span))  # type: ignore

    async def start(self, arg: U):
        """
        Note:
        - Only first method call starts task. Subsequent calls do nothing.
        """
        assert not isinstance(arg, ray._raylet.ObjectRef)

        # task computation should only be run once. So, if task has started do nothing.
        if self.has_started():
            return

        self._start_called = True

        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("execute-task") as span:
            try:
                self._set_span_id(span)

                # pre-task
                for k, v in self._attributes.items():
                    span.set_attribute(k, v)

                # wait for task to finish
                f_result: A = await self._f_remote(arg)

                # post-task
                task_result = self._combiner(span, Try(f_result, None))
            except BaseException as e:
                task_result = self._combiner(span, Try(None, e))

        # note that result is set before callbacks are called
        self._set_result(task_result)

        await asyncio.gather(*[cb(task_result) for cb in self._on_complete_callbacks])

    def has_started(self) -> bool:
        return self._start_called

    async def get_task_result(self) -> B:
        """
        Returns (an awaitable that resolves to) this task's return value.

        Notes:
        - Method can be called either before or after the task has started.
        """
        return await self._future_result

    async def get_span_id(self) -> str:
        """
        Returns (an awaitable that resolves to) this task's OpenTelemetry span_id
        once the task has started and a span_id is assigned.

        Notes:
        - Method can be called either before or after the task has started.
        """
        return await self._future_span_id

    def has_completed(self) -> bool:
        """
        Returns True/False whether task has completed.

        Notes:
        - True value does not imply that on_complete-callbacks have been called
        or finished.
        - Method can be called either before or after the task has started.
        """
        return self._future_result.done()


def _task_from_remote_f(
    f_remote: Callable[[U], Awaitable[Try[B]]],
    task_type: str,
    attributes: AttributesDict = {},
    fail_message: str = "Remote function call failed",
) -> RemoteTaskP[U, TaskOutcome[B]]:
    def _combiner(span: Span, b: Try[B]) -> TaskOutcome[B]:
        span_id = get_span_hexid(span)
        if b.is_success():
            span.set_status(Status(StatusCode.OK))
            return TaskOutcome(span_id=span_id, return_value=b.value, error=None)
        else:
            span.record_exception(b.error)
            span.set_status(Status(StatusCode.ERROR, fail_message))

            return TaskOutcome(span_id=span_id, return_value=None, error=b.error)

    # TODO: ... rewrite later by refactoring GenTask_OT constructor ...
    async def untry_f(u: U) -> B:
        try_fu: Try[B] = await f_remote(u)
        if try_fu.is_success():
            return try_fu.get()
        else:
            raise try_fu.error  # type: ignore

    if "task_type" in attributes:
        raise ValueError("task_type key should not be included in tags")

    return GenTask_OT.remote(
        f_remote=untry_f,
        combiner=_combiner,
        attributes={**attributes, "task.task_type": task_type},
    )


def task_from_python_function(
    f: Callable[[U], B],
    num_cpus: int = 1,
    max_nr_retries: int = 1,
    timeout_s: Optional[float] = None,
    attributes: AttributesDict = {},
    task_type: str = "Python",
) -> RemoteTaskP[U, TaskOutcome[B]]:
    """
    Lift a Python function f (U -> B) into a Task.
    """
    try_f_remote: Callable[[U], Awaitable[Try[B]]] = try_f_with_timeout_guard(
        f=f, timeout_s=timeout_s, num_cpus=num_cpus
    )

    try_f_remote_wrapped: Callable[[U], Awaitable[Try[B]]] = retry_wrapper_ot(
        try_f_remote, max_nr_retries=max_nr_retries
    )

    return _task_from_remote_f(
        try_f_remote_wrapped, attributes=attributes, task_type=task_type
    )


def _cb_compose_tasks(
    task1: RemoteTaskP[U, TaskOutcome[A]],
    task2: RemoteTaskP[TaskOutcome[A], TaskOutcome[B]],
):
    """
    Add on_complete callback to task1 so that:
      - task2 starts when task1 finishes
      - log (task1 -> task2) dependency

    TODO: error handling
    """

    async def task1_on_complete_handler(task1_result: TaskOutcome[A]) -> None:
        assert await task1.has_started.remote() == True
        assert await task1.has_completed.remote() == True

        task1_span_id = await task1.get_span_id.remote()
        task2_awaitable = task2.start.remote(task1_result)  # type: ignore
        task2_span_id = await task2.get_span_id.remote()

        # After span_id:s of Task1 and Task2 are known, log that these have a
        # sequential dependence
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("task-dependency") as span:
            span.set_attribute("from_task_span_id", task1_span_id)
            span.set_attribute("to_task_span_id", task2_span_id)

        await task2_awaitable

    ray.get(task1.add_callback.remote(task1_on_complete_handler))  # type: ignore


def run_in_sequence(*tasks: RemoteTaskP[TaskOutcome[A], TaskOutcome[A]]):
    """
    Execute a list of tasks in sequence. The output of each task is passed as the
    argument to the next task in the sequence.

    Eg.
        task1 -> task2 -> task3

    """
    if len(tasks) <= 1:
        raise ValueError("Need at least two input tasks")
    else:
        for task1, task2 in pairs(tasks):
            _cb_compose_tasks(task1, task2)


def fan_in(
    paralllel_tasks: List[RemoteTaskP[TaskOutcome[A], TaskOutcome[B]]],
    target_task: RemoteTaskP[List[TaskOutcome[B]], TaskOutcome[C]],
):
    """
    Add on_complete callbacks to tasks in `paralllel_tasks` so that `target_task`-task
    is started when all tasks in `paralllel_tasks` have completed.

    Eg.,

       task1 ---\
                 \
                  v
       task2 ------> target_task
                  ^
                 /
       task3 ---/

    Also log these task dependencies after target_task is started.
    """
    if len(paralllel_tasks) == 0:
        raise ValueError("Called with zero length task list.")

    if target_task in paralllel_tasks:
        raise ValueError("Task listed in both arguments of fan_in")

    @ray.remote(num_cpus=0)
    class TargetTaskTrigger:
        def __init__(self):
            self._completed_tasks = []

        async def _start_target_task(self):
            parallel_tasks_results: List[TaskOutcome[B]] = [
                await task.get_task_result.remote() for task in paralllel_tasks
            ]
            target_task_awaitable = target_task.start.remote(parallel_tasks_results)  # type: ignore

            target_span_id = await target_task.get_span_id.remote()

            # Log task dependencies
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            for task in self._completed_tasks:
                with tracer.start_as_current_span("task-dependency") as span:
                    span.set_attribute(
                        "from_task_span_id", await task.get_span_id.remote()
                    )
                    span.set_attribute("to_task_span_id", target_span_id)

            await target_task_awaitable

        async def record_completed_task(self, task):
            self._completed_tasks.append(task)

            if len(self._completed_tasks) == len(paralllel_tasks):
                await self._start_target_task()

    target_task_trigger = TargetTaskTrigger.remote()  # type: ignore

    for task in paralllel_tasks:

        async def task_on_complete_handler(task_result: TaskOutcome[B]) -> None:
            await target_task_trigger.record_completed_task.remote(task)

        ray.get(task.add_callback.remote(task_on_complete_handler))  # type: ignore


def start_and_await_tasks(
    tasks_to_start: List[RemoteTaskP[A, A]],
    tasks_to_await: List[RemoteTaskP[A, A]],
    timeout_s: float = None,
    arg=None,
) -> List[A]:
    """
    Start the provided list of tasks and return when the `task_to_await` task has
    finished.

    timeout_s: Optionally limit compute time with timeout with values
    None (no timeout), or timeout in seconds

    Returns:
      return values of tasks in `task_to_await`

    Notes:
    - TODO check that await tasks have no callbacks. Such callbacks need to start/finish
    before this function returns.
    """
    assert isinstance(tasks_to_start, list)
    assert isinstance(tasks_to_await, list)

    if len(tasks_to_start) == 0:
        raise ValueError("No tasks to start")

    if len(tasks_to_await) == 0:
        raise ValueError("No tasks to await")

    for task in tasks_to_start:
        task.start.remote(arg)

    return ray.get(
        [task.get_task_result.remote() for task in tasks_to_await], timeout=timeout_s
    )
