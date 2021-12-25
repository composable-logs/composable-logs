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
from pynb_dag_runner.helpers import one, pairs
from pynb_dag_runner.core.dag_syntax import Node, Edge, Edges
from pynb_dag_runner.ray_helpers import Future, FutureActor, RayMypy
from pynb_dag_runner.opentelemetry_helpers import SpanId, get_span_hexid

A = TypeVar("A")
B = TypeVar("B")
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


class Try(Generic[A]):
    def __init__(self, value: Optional[A], error: Optional[Exception]):
        assert value is None or error is None
        self.value = value
        self.error = error

    def get(self) -> A:
        if self.error is not None:
            raise Exception(f"Try does not contain any value (err={self.error})")
        return self.value  # type: ignore


X = TypeVar("X", contravariant=True)
Y = TypeVar("Y", covariant=True)


class TaskP(Protocol[X, Y]):
    def start(self, *args: X) -> None:
        ...

    def get_result(self) -> Awaitable[Y]:
        ...


# We can not directly use TaskP-protocol since our Task-class will be remote Ray actor.
# The below encode a Ray actor with the above (remote) class methods.


class _RemoteTaskP_set(Protocol[X]):
    def remote(self, *args: X) -> None:
        ...


class _RemoteTaskP_get(Protocol[Y]):
    def remote(self) -> Awaitable[Y]:
        ...


class RemoteTaskP(Protocol[X, Y]):
    @property
    def start(self) -> _RemoteTaskP_set[X]:
        ...

    @property
    def add_callback(self) -> _RemoteTaskP_set[Callable[[Y], Awaitable[None]]]:
        ...

    @property
    def get_task_result(self) -> _RemoteTaskP_get[Y]:
        ...

    @property
    def get_span_id(self) -> _RemoteTaskP_get[str]:
        ...

    @property
    def has_started(self) -> _RemoteTaskP_get[bool]:
        ...

    @property
    def has_completed(self) -> _RemoteTaskP_get[bool]:
        ...


@ray.remote(num_cpus=0)
class GenTask_OT(Generic[U, A, B], TaskP[U, B], RayMypy):
    """
    Represent a task that a can be run once
    """

    def __init__(
        self,
        f_remote: Callable[..., Awaitable[A]],  # ... = [U, ..., U]
        combiner: Callable[[Span, Try[A]], B],
        on_complete_callbacks: List[Callable[[B], Awaitable[None]]] = [],
        tags: TaskTags = {},
    ):
        self._f_remote: Callable[..., Awaitable[A]] = f_remote
        self._combiner: Callable[[Span, Try[A]], B] = combiner
        self._future_or_none: Optional[Awaitable[B]] = None
        self._on_complete_callbacks: List[
            Callable[[B], Awaitable[None]]
        ] = on_complete_callbacks
        self._span_id_future: FutureActor = FutureActor.remote()  # type: ignore
        self._tags: TaskTags = tags

    def add_callback(self, cb: Callable[[B], Awaitable[None]]) -> None:
        if self.has_started():
            raise Exception("Cannot add callbacks once task has started")

        self._on_complete_callbacks.append(cb)

    async def get_span_id(self) -> str:
        return await self._span_id_future.wait.remote()  # type: ignore

    def _set_span_id(self, span: Span):
        self._span_id_future.set_value.remote(get_span_hexid(span))  # type: ignore

    def start(self, *args: U) -> None:
        assert not any(isinstance(s, ray._raylet.ObjectRef) for s in args)

        # task computation should only be run once. So, if task has started do nothing.
        if self.has_started():
            return

        async def make_call(*_args: U) -> B:
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
                    f_result: A = await self._f_remote(*_args)

                    # post-task
                    task_result = self._combiner(span, Try(f_result, None))
                except Exception as e:
                    task_result = self._combiner(span, Try(None, e))

            for cb in self._on_complete_callbacks:
                await cb(task_result)

            return task_result

        self._future_or_none = Future.lift_async(make_call)(*args)  # non-blocking

    async def get_task_result(self) -> B:
        """
        Returns an awaitable that resolves to this task's return value. This method can
        be called either before or after the task has started.
        """
        # wait for task to have started
        _ = await self.get_span_id()

        # wait for task computation to finish
        assert self._future_or_none is not None  # (for mypy to rule out None value)
        return await self._future_or_none

    def has_started(self) -> bool:
        return self._future_or_none is not None

    def has_completed(self) -> bool:
        if self._future_or_none is None:
            return False

        # See: https://docs.ray.io/en/master/package-ref.html#ray-wait
        finished_refs, not_finished_refs = ray.wait([self._future_or_none], timeout=0)
        assert len(finished_refs) + len(not_finished_refs) == 1

        return len(finished_refs) == 1


def task_from_remote_f(
    f_remote: Callable[..., Awaitable[B]],
    tags: TaskTags = {},
    fail_message: str = "Remote function call failed",
) -> RemoteTaskP[U, TaskOutcome[B]]:
    """
    Lift a Ray remote function f_remote(*args: A) -> B into a Task[TaskOutcome[B]].
    """

    def _combiner(span: Span, b: Try[B]) -> TaskOutcome[B]:
        span_id = get_span_hexid(span)
        if b.error is None:
            span.set_status(Status(StatusCode.OK))
            return TaskOutcome(span_id=span_id, return_value=b.value, error=None)
        else:
            span.record_exception(b.error)
            span.set_status(Status(StatusCode.ERROR, fail_message))

            return TaskOutcome(span_id=span_id, return_value=None, error=b.error)

    return GenTask_OT.remote(
        f_remote=Future.lift_async(f_remote), combiner=_combiner, tags=tags
    )


def task_from_func(
    f: Callable[..., TaskOutcome[B]], num_cpus: int = 0, tags: TaskTags = {}
) -> RemoteTaskP[U, TaskOutcome[B]]:
    """
    Lift a Python function f(*args: A) -> TaskOutcome[B] into a Task[TaskOutcome[B]].

    Note: this (and task_from_remote_f) are not a class methods for GenTask_OT since
    we return a Task[TaskOutcome[B]] and not a Task[A].
    """

    @ray.remote(num_cpus=num_cpus)
    def remote_f(*args: A) -> TaskOutcome[B]:
        return f(*args)

    return task_from_remote_f(remote_f.remote, tags=tags)


def _compose_two_tasks_in_sequence(
    task1: RemoteTaskP[U, TaskOutcome[A]],
    task2: RemoteTaskP[TaskOutcome[A], TaskOutcome[B]],
) -> RemoteTaskP[U, TaskOutcome[B]]:
    async def run_tasks_in_sequence(*task1_arguments: U) -> TaskOutcome[B]:
        assert not any(
            isinstance(arg, ray._raylet.ObjectRef) for arg in task1_arguments
        )
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("task-dependency") as span:
            task1.start.remote(*task1_arguments)
            outcome1 = await task1.get_task_result.remote()

            # TODO: second task should probably be skipped if first one fails
            task2.start.remote(outcome1)
            outcome2 = await task2.get_task_result.remote()

            span.set_attribute("from_task_span_id", outcome1.span_id)
            span.set_attribute("to_task_span_id", outcome2.span_id)

            return outcome2

    def _combiner(span: Span, b: Try[TaskOutcome[B]]) -> TaskOutcome[B]:
        return b.get()

    return GenTask_OT.remote(
        f_remote=Future.lift_async(run_tasks_in_sequence), combiner=_combiner
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


def in_sequence(
    *tasks: RemoteTaskP[TaskOutcome[A], TaskOutcome[A]]
) -> RemoteTaskP[TaskOutcome[A], TaskOutcome[A]]:
    """
    Execute a list of tasks in sequence. The output of each task is passed as the
    argument to the next task in the sequence.

    Eg.
        task1 -> task2 -> task3

    TODO: error handling
    """
    if len(tasks) == 0:
        raise ValueError("Empty task list provided")
    else:
        first, *rest = tasks
        if len(rest) == 0:
            return first
        else:
            return _compose_two_tasks_in_sequence(first, in_sequence(*rest))


def in_parallel(
    *tasks: RemoteTaskP[TaskOutcome[A], TaskOutcome[B]],
) -> RemoteTaskP[TaskOutcome[A], TaskOutcome[List[TaskOutcome[B]]]]:
    """
    Return new task that runs the provided tasks in parallel (with available resources)
     - The return value of new task is the list of return values of provided input tasks
       (see type signature).
     - Arguments passed to the combined task is passed to all tasks when calling start()
     - The returned task fails if any of the input tasks fail.
     - The span_id:s for the tasks run in parallel are logged.
    """

    async def run_tasks_in_parallel(
        *task_arguments: TaskOutcome[A],
    ) -> List[TaskOutcome[A]]:
        for task in tasks:
            task.start.remote(*task_arguments)

        return await asyncio.gather(*[task.get_task_result.remote() for task in tasks])

    async def flatten_results(
        *task_arguments: TaskOutcome[A],
    ) -> TaskOutcome[List[TaskOutcome[A]]]:
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("run-in-parallel") as span:
            task_outcomes: List[TaskOutcome[A]] = await run_tasks_in_parallel(
                *task_arguments
            )

            span_ids = [task_outcome.span_id for task_outcome in task_outcomes]
            span.set_attribute("span_ids", span_ids)

            assert all(isinstance(s, str) for s in span_ids)

            # check for errors in any of the tasks
            failed_span_ids = [
                task_outcome.span_id
                for task_outcome in task_outcomes
                if task_outcome.error is not None
            ]
            exception: Optional[Exception] = None
            if len(failed_span_ids) > 0:
                exception = Exception(
                    "Failed to run span_id:s " + ",".join(failed_span_ids)
                )
                span.record_exception(exception)

            span_id = format_span_id(span.get_span_context().span_id)
            return TaskOutcome(
                span_id=span_id,
                return_value=task_outcomes,
                error=exception,
            )

    def _combiner(span: Span, b: Try[TaskOutcome[B]]) -> TaskOutcome[B]:
        return b.get()

    return GenTask_OT.remote(
        f_remote=Future.lift_async(flatten_results), combiner=_combiner
    )


def run_and_await_tasks(
    tasks_to_run: List[RemoteTaskP],
    task_to_await: RemoteTaskP[A, B],
    timeout_s: float = None,
) -> B:
    """
    Start the provided list of tasks, return when the `task_to_await` task has finished.

    Optionally limit compute time with timeout
    None (no timeout), or timeout in seconds

    Returns:
      return value of `task_to_await`
    """
    assert len(tasks_to_run) > 0

    for task in tasks_to_run:
        task.start.remote()

    return ray.get(task_to_await.get_task_result.remote(), timeout=timeout_s)


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
