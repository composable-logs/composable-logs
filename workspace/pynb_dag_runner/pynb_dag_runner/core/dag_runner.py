from dataclasses import dataclass
from typing import Awaitable, List, Optional, Any, TypeVar, Generic, Callable

#
import ray
import opentelemetry as otel
from opentelemetry.trace.span import format_span_id

#
from pynb_dag_runner.helpers import one
from pynb_dag_runner.core.dag_syntax import Node, Edge, Edges
from pynb_dag_runner.ray_helpers import Future

A = TypeVar("A")


class Task(Node, Generic[A]):
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


@dataclass
class TaskOutcome(Generic[A]):
    span_id: SpanId
    return_value: A
    error: Optional[Exception]


class Task_OT(Generic[A]):
    def __init__(self, f_remote: Callable[..., Future[TaskOutcome[A]]]):
        self._f_remote = f_remote
        self._future_or_none: Optional[Future[TaskOutcome[A]]] = None

    def start(self, *args: Any) -> None:
        """
        Start execution of task and return
        """
        if self.has_started():
            raise Exception(f"Task has already started")

        assert all(isinstance(s, ray._raylet.ObjectRef) for s in args)

        async def make_call():
            result = await self._f_remote(*args)
            assert isinstance(result, TaskOutcome)
            return result

        self._future_or_none = Future.lift_async(make_call)()

    def get_ref(self) -> Awaitable[TaskOutcome[A]]:
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

    @classmethod
    def from_remote_f(cls, f_remote: Callable[..., Awaitable[A]]) -> "Task_OT[A]":
        async def make_call(*args):
            tracer = otel.trace.get_tracer(__name__)  # type: ignore
            with tracer.start_as_current_span("eval-in-otel-span") as span:
                span_id = format_span_id(span.get_span_context().span_id)
                try:
                    return TaskOutcome(
                        span_id=span_id,
                        return_value=await f_remote(*args),
                        error=None,
                    )
                except Exception as e:
                    return TaskOutcome(span_id=span_id, return_value=None, error=e)

        return cls(f_remote=Future.lift_async(make_call))

    @classmethod
    def from_f(cls, f: Callable[..., A], num_cpus: int = 0) -> "Task_OT[A]":
        @ray.remote(num_cpus=num_cpus)
        def remote_f(*args):
            return f(*args)

        return cls.from_remote_f(remote_f.remote)


def _compose_two_tasks_in_sequence(task1: Task_OT[A], task2: Task_OT[A]) -> Task_OT[A]:
    async def run_tasks_in_sequence(*task1_arguments: Any):
        assert not any(
            isinstance(arg, ray._raylet.ObjectRef) for arg in task1_arguments
        )
        tracer = otel.trace.get_tracer(__name__)  # type: ignore
        with tracer.start_as_current_span("task-dependency") as span:
            task1.start(*[ray.put(arg) for arg in task1_arguments])
            outcome1 = await task1.get_ref()
            task2.start(ray.put(outcome1))
            outcome2 = await task2.get_ref()

            span.set_attribute("from_span_id", outcome1.span_id)
            span.set_attribute("to_span_id", outcome2.span_id)

            return outcome2

    return Task_OT(f_remote=Future.lift_async(run_tasks_in_sequence))


def in_sequence(*tasks: Task_OT[A]) -> Task_OT[A]:
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
