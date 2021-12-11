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

#
from pynb_dag_runner.helpers import one
from pynb_dag_runner.core.dag_syntax import Node, Edge, Edges
from pynb_dag_runner.ray_helpers import Future

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


SpanId = str
TaskTags = Mapping[str, str]


@dataclass(frozen=True, eq=True)
class TaskOutcome(Generic[A]):
    span_id: SpanId
    return_value: Optional[A]
    error: Optional[Exception]


class Task_OT(Generic[A]):
    # --- deprecated ---
    def __init__(
        self,
        f_remote: Callable[..., Awaitable[A]],
        tags: TaskTags = {},
    ):
        self._f_remote: Callable[..., Awaitable[A]] = f_remote
        self._future_or_none: Optional[Awaitable[A]] = None
        self._tags: TaskTags = tags

    def start(self, *args: Any) -> None:
        """
        Start execution of task and return
        """
        assert not any(isinstance(s, ray._raylet.ObjectRef) for s in args)

        if self.has_started():
            raise Exception(f"Task has already started")

        async def make_call(*_args: Any):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("execute-task") as span:
                try:
                    result = await self._f_remote(*_args)
                    for k, v in self._tags.items():
                        span.set_attribute(k, v)

                except Exception as e:
                    raise e

            return result

        self._future_or_none = Future.lift_async(make_call)(*args)

    def get_ref(self) -> Awaitable[A]:
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

    def get_ref(self) -> Awaitable[Y]:
        ...


class GenTask_OT(Generic[U, A, B], TaskP[U, B]):
    def __init__(
        self,
        f_remote: Callable[..., Awaitable[A]],  # ... = [U, ..., U]
        combiner: Callable[[Span, Try[A]], B],
        tags: TaskTags = {},
    ):
        self._f_remote: Callable[..., Awaitable[A]] = f_remote
        self._combiner: Callable[[Span, Try[A]], B] = combiner
        self._future_or_none: Optional[Awaitable[B]] = None
        self._tags: TaskTags = tags

    def start(self, *args: U) -> None:
        assert not any(isinstance(s, ray._raylet.ObjectRef) for s in args)

        if self.has_started():
            raise Exception(f"Tasks can only be run once, and this has already started")

        async def make_call(*_args: Any) -> B:
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("execute-task") as span:
                for k, v in self._tags.items():
                    span.set_attribute(f"tags.{k}", v)
                try:
                    result: A = await self._f_remote(*_args)
                    return self._combiner(span, Try(result, None))
                except Exception as e:
                    span.record_exception(e)
                    return self._combiner(span, Try(None, e))

        self._future_or_none = Future.lift_async(make_call)(*args)

    def get_ref(self) -> Awaitable[B]:
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


def task_from_remote_f(
    f_remote: Callable[..., Awaitable[B]], tags: TaskTags = {}
) -> TaskP[U, TaskOutcome[B]]:
    """
    Lift a Ray remote function f_remote(*args: A) -> B into a Task[TaskOutcome[B]].
    """

    def _combiner(span: Span, b: Try[B]) -> TaskOutcome[B]:
        # manually add "0x" to be compatible with OTEL json:s
        span_id = "0x" + format_span_id(span.get_span_context().span_id)
        if b.error is None:
            return TaskOutcome(span_id=span_id, return_value=b.value, error=None)
        else:
            return TaskOutcome(span_id=span_id, return_value=None, error=b.error)

    return GenTask_OT(
        f_remote=Future.lift_async(f_remote), combiner=_combiner, tags=tags
    )


def task_from_func(
    f: Callable[..., TaskOutcome[B]], num_cpus: int = 0, tags: TaskTags = {}
) -> TaskP[U, TaskOutcome[B]]:
    """
    Lift a Python function f(*args: A) -> TaskOutcome[B] into a Task[TaskOutcome[B]].

    Note: this (and task_from_remote_f) are not a class methods for Task_OT since
    we return a Task[TaskOutcome[B]] and not a Task[A].
    """

    @ray.remote(num_cpus=num_cpus)
    def remote_f(*args: A) -> TaskOutcome[B]:
        return f(*args)

    return task_from_remote_f(remote_f.remote, tags=tags)


def _compose_two_tasks_in_sequence(
    task1: TaskP[U, TaskOutcome[A]],
    task2: TaskP[TaskOutcome[A], TaskOutcome[B]],
) -> TaskP[U, TaskOutcome[B]]:
    async def run_tasks_in_sequence(*task1_arguments: U) -> TaskOutcome[B]:
        assert not any(
            isinstance(arg, ray._raylet.ObjectRef) for arg in task1_arguments
        )
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("task-dependency") as span:
            task1.start(*task1_arguments)
            outcome1 = await task1.get_ref()

            # TODO: second task should probably be skipped if first one fails
            task2.start(outcome1)
            outcome2 = await task2.get_ref()

            span.set_attribute("from_task_span_id", outcome1.span_id)
            span.set_attribute("to_task_span_id", outcome2.span_id)

            return outcome2

    def _combiner(span: Span, b: Try[TaskOutcome[B]]) -> TaskOutcome[B]:
        return b.get()

    return GenTask_OT(
        f_remote=Future.lift_async(run_tasks_in_sequence), combiner=_combiner
    )


def in_sequence(
    *tasks: TaskP[TaskOutcome[A], TaskOutcome[A]]
) -> TaskP[TaskOutcome[A], TaskOutcome[A]]:
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
    *tasks: Task_OT[TaskOutcome[A]],
) -> Task_OT[TaskOutcome[List[TaskOutcome[A]]]]:
    """
    Return new task that runs the provided tasks in parallel (with available resources)
     - The return value of new task is the list of return values of provided input tasks
       (see type signature).
     - Arguments passed to the combined task is passed to all tasks when calling start()
     - The returned task fails if any of the input tasks fail.
     - The span_id:s for the tasks run in parallel are logged.
    """

    async def run_tasks_in_parallel(*task_arguments: U) -> List[TaskOutcome[A]]:
        for task in tasks:
            task.start(*task_arguments)

        return await asyncio.gather(*[task.get_ref() for task in tasks])

    async def flatten(*task_arguments: U) -> TaskOutcome[List[TaskOutcome[A]]]:
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("run-in-parallel") as span:
            task_outcomes: List[TaskOutcome[A]] = await run_tasks_in_parallel(
                *task_arguments
            )

            span_ids = [task_outcome.span_id for task_outcome in task_outcomes]
            span.set_attribute("span_ids", span_ids)

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

    return Task_OT(f_remote=Future.lift_async(flatten))


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
