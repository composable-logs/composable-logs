import asyncio
from dataclasses import dataclass
from typing import (
    Awaitable,
    List,
    Optional,
    Any,
    TypeVar,
    Generic,
    Callable,
    Mapping,
    Protocol,
)

#
import ray
import opentelemetry as otel
from opentelemetry.trace.span import format_span_id, Span
from opentelemetry.trace import StatusCode, Status  # type: ignore

#
from pynb_dag_runner.helpers import one, pairs, Try, compose
from pynb_dag_runner.ray_helpers import try_f_with_timeout_guard
from pynb_dag_runner.core.dag_syntax import Node, Edge, Edges
from pynb_dag_runner.ray_helpers import (
    Future,
    FutureActor,
    RayMypy,
    try_f_with_timeout_guard,
    retry_wrapper_ot,
)
from pynb_dag_runner.ray_mypy_helpers import RemoteGetFunction, RemoteSetFunction
from pynb_dag_runner.opentelemetry_helpers import (
    SpanId,
    get_span_hexid,
    otel_add_baggage,
)

A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")
U = TypeVar("U")
V = TypeVar("V")
W = TypeVar("W")


class Task(Node, Generic[A]):
    # --- deprecated ---
    # all below methods are non-blocking

    def __init__(self, f_remote: Callable[..., Future[A]]):
        self._f_remote = f_remote
        self._future_or_none: Optional[Future[A]] = None

    def start(self, *args: Any) -> None:
        """
        Start execution of task and return
        """
        if self.has_started():
            raise Exception(f"Task has already started")

        self._future_or_none = self._f_remote(*args)

    def get_ref(self) -> Future[A]:
        if not self.has_started():
            raise Exception(f"Task has not started")

        return self._future_or_none  # type: ignore

    def has_started(self) -> bool:
        return self._future_or_none is not None

    def has_completed(self) -> bool:
        if not self.has_started():
            return False

        # See: https://docs.ray.io/en/master/package-ref.html#ray-wait
        finished_refs, not_finished_refs = ray.wait([self.get_ref()], timeout=0)
        assert len(finished_refs) + len(not_finished_refs) == 1

        return len(finished_refs) == 1

    def result(self) -> A:
        """
        Get result if task has already completed. Otherwise raise an exception.
        """
        if not self.has_completed():
            raise Exception(
                "Result has not finished. Calling ray.get would block execution"
            )

        return ray.get(self.get_ref())


TaskTags = Mapping[str, str]


@dataclass(frozen=True, eq=True)
class TaskOutcome(Generic[A]):
    span_id: SpanId
    return_value: Optional[A]
    error: Optional[Exception]


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
        tags: TaskTags = {},
    ):
        self._f_remote: Callable[[U], Awaitable[A]] = f_remote
        self._combiner: Callable[[Span, Try[A]], B] = combiner
        self._span_id_future: FutureActor = FutureActor.remote()  # type: ignore
        self._future_value: FutureActor = FutureActor.remote()  # type: ignore
        self._on_complete_callbacks: List[
            Callable[[B], Awaitable[None]]
        ] = on_complete_callbacks
        self._tags: TaskTags = tags
        self._start_called = False

    def add_callback(self, cb: Callable[[B], Awaitable[None]]) -> None:
        """
        Note:
        - Method can only be called before first call to start-method.
        """
        if self.has_started():
            raise Exception("Cannot add callbacks once task has started")

        self._on_complete_callbacks.append(cb)

    def _set_result(self, value: B):
        self._future_value.set_value.remote(value)  # type: ignore

    def _set_span_id(self, span: Span):
        self._span_id_future.set_value.remote(get_span_hexid(span))  # type: ignore

    def start(self, arg: U) -> None:
        """
        Note:
        - Only first method call starts task. Subsequent calls do nothing.
        """
        assert not isinstance(arg, ray._raylet.ObjectRef)

        # task computation should only be run once. So, if task has started do nothing.
        if self.has_started():
            return

        self._start_called = True

        async def make_call(_arg: U):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("execute-task") as span:
                try:
                    # Note:
                    # This function is run as a remote Ray function with self being a
                    # copy of the Task actor; Thus, any changes like self.x = 123 will
                    # not update the main actor.
                    #
                    # Reading self is possible, and updating state on remote actors
                    # referenced from self works, as done one the next line.
                    self._set_span_id(span)

                    # pre-task
                    for k, v in self._tags.items():
                        span.set_attribute(f"tags.{k}", v)

                    # wait for task to finish
                    f_result: A = await self._f_remote(_arg)

                    # post-task
                    task_result = self._combiner(span, Try(f_result, None))
                except Exception as e:
                    task_result = self._combiner(span, Try(None, e))

            # note, result is set before callbacks are called
            self._set_result(task_result)

            for cb in self._on_complete_callbacks:
                await cb(task_result)

        # Start computation in other (possibly remote) Python process, non-blocking call
        Future.lift_async(make_call)(arg)

    def has_started(self) -> bool:
        return self._start_called

    async def get_task_result(self) -> B:
        """
        Returns (an awaitable that resolves to) this task's return value.

        Notes:
        - Method can be called either before or after the task has started.
        """
        return await self._future_value.wait.remote()  # type: ignore

    async def get_span_id(self) -> str:
        """
        Returns (an awaitable that resolves to) this task's OpenTelemetry span_id
        once the task has started and a span_id is assigned.

        Notes:
        - Method can be called either before or after the task has started.
        """
        return await self._span_id_future.wait.remote()  # type: ignore

    async def has_completed(self) -> bool:
        """
        Returns True/False whether task has completed.

        Notes:
        - True value does not imply that on_complete-callbacks have been called
        or finished.
        - Method can be called either before or after the task has started.
        """
        return await self._future_value.value_is_set.remote()  # type: ignore


def _task_from_remote_f(
    f_remote: Callable[[U], Awaitable[Try[B]]],
    task_type: str,
    tags: TaskTags = {},
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
            raise try_fu.error

    if "task_type" in tags:
        raise ValueError("task_type key should not be included in tags")

    return GenTask_OT.remote(
        f_remote=Future.lift_async(untry_f),
        combiner=_combiner,
        tags={**tags, "task_type": task_type},
    )


def task_from_python_function(
    f: Callable[[U], B],
    num_cpus: int = 1,
    max_nr_retries: int = 1,
    timeout_s: Optional[float] = None,
    tags: TaskTags = {},
) -> RemoteTaskP[U, TaskOutcome[B]]:
    """
    Lift a Python function f (U -> B) into a Task.
    """
    try_f_remote: Callable[
        [Awaitable[U]], Awaitable[Try[B]]
    ] = try_f_with_timeout_guard(f=f, timeout_s=timeout_s, num_cpus=num_cpus)

    try_f_remote_put: Callable[[U], Awaitable[Try[B]]] = compose(try_f_remote, ray.put)

    try_f_remote_wrapped: Callable[[U], Awaitable[Try[B]]] = retry_wrapper_ot(
        try_f_remote_put, max_nr_retries=max_nr_retries
    )

    return _task_from_remote_f(try_f_remote_wrapped, tags=tags, task_type="Python")


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
        task2.start.remote(task1_result)
        task2_span_id = await task2.get_span_id.remote()

        # After span_id:s of Task1 and Task2 are known, log that these have a
        # sequential dependence
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("task-dependency") as span:
            span.set_attribute("from_task_span_id", task1_span_id)
            span.set_attribute("to_task_span_id", task2_span_id)

    task1.add_callback.remote(task1_on_complete_handler)


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

    @ray.remote(num_cpus=0)
    class TargetTaskTrigger:
        def __init__(self):
            self._completed_tasks = []

        async def start_target_task(self):
            parallel_tasks_results: List[TaskOutcome[B]] = [
                await task.get_task_result.remote() for task in paralllel_tasks
            ]
            target_task.start.remote(parallel_tasks_results)

            target_span_id = await target_task.get_span_id.remote()

            # Log task dependencies
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            for task in self._completed_tasks:
                with tracer.start_as_current_span("task-dependency") as span:
                    span.set_attribute(
                        "from_task_span_id", await task.get_span_id.remote()
                    )
                    span.set_attribute("to_task_span_id", target_span_id)

        async def record_completed_task(self, task):
            self._completed_tasks.append(task)

            if len(self._completed_tasks) == len(paralllel_tasks):
                await self.start_target_task()

    target_task_trigger = TargetTaskTrigger.remote()  # type: ignore

    for task in paralllel_tasks:

        async def task_on_complete_handler(task_result: TaskOutcome[B]) -> None:
            await target_task_trigger.record_completed_task.remote(task)

        task.add_callback.remote(task_on_complete_handler)


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


## v--- deprecated ---v


class TaskDependence(Edge):
    pass


class TaskDependencies(Edges):
    pass


def _task_can_run(
    task: Task[A],
    completed_tasks: List[Task[A]],
    task_dependencies: TaskDependencies,
) -> bool:
    """
    Assume that tasks in completed_tasks have completed.

    Return true/false depending whether all dependencies for task have completed.
    """

    # get all dependencies that restrict whether task can start
    required_dependencies = [
        td.from_node for td in task_dependencies if task == td.to_node
    ]

    # task can run if all dependent tasks have completed
    return all(
        dependent_task in completed_tasks for dependent_task in required_dependencies
    )


def _get_next_completed_task(tasks: List[Task[A]]) -> Task[A]:
    """
    Block and wait for the next task to finish. Return that task.
    """
    assert len(tasks) > 0
    ref_to_task_dict = {t.get_ref(): t for t in tasks}

    refs_done, _ = ray.wait(list(ref_to_task_dict.keys()), num_returns=1, timeout=None)
    assert len(refs_done) == 1

    return ref_to_task_dict[refs_done[0]]


def run_tasks(all_tasks: List[Task[A]], task_dependencies: TaskDependencies) -> List[A]:
    """
    Run tasks listed in `all_tasks` subject to order constraints in `task_dependencies`.
    Return the results of the tasks as a list (in an unspecified order).

    Notes:
    As an alternative to the below brute force implementation, one could possibly use
    a topological sort to determine run order. Eg. the below code has no checks for
    graph cycles.

    Python 3.9 has a native graphlib
    https://docs.python.org/3/library/graphlib.html
    """
    assert all(not task.has_started() for task in all_tasks)

    completed_tasks: List[Task[A]] = []
    not_completed_tasks: List[Task[A]] = all_tasks

    iteration_nr: int = 0
    while len(not_completed_tasks) > 0:
        iteration_nr += 1

        print(f"At iteration {iteration_nr}/{len(all_tasks)} ...")

        not_completed_tasks_that_can_run = [
            task
            for task in not_completed_tasks
            if _task_can_run(task, completed_tasks, task_dependencies)
        ]

        if iteration_nr == 0 and len(not_completed_tasks_that_can_run) == 0:
            raise Exception("Check run dependencies, unable to start any task!")

        # ensure that all tasks that can run are started
        for task in not_completed_tasks_that_can_run:
            if not task.has_started():
                task.start(ray.put({}))

        # wait for next task to complete
        next_completed_task: Task[A] = _get_next_completed_task(
            not_completed_tasks_that_can_run
        )

        # update completed/not_completed task lists
        completed_tasks += [next_completed_task]
        not_completed_tasks = [
            task for task in all_tasks if task not in completed_tasks
        ]

    assert all(task.has_completed() for task in all_tasks)
    return [t.result() for t in completed_tasks]
