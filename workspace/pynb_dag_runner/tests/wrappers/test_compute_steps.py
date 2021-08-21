#
import ray

#
from pynb_dag_runner.helpers import compose
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.core.dag_runner import Task
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.wrappers.compute_steps import T, AddParameters, AddDynamicParameter


# define identity transformation Future[Runlog] -> Future[Runlog]
id_tf: T[Future[Runlog]] = Future.lift(lambda runlog: runlog)


def test_pipeline_no_compute_steps():
    # check expected output for task that does nothing to runlog

    task = Task(id_tf)
    task.start(Future.value(Runlog()))

    assert ray.get(task.get_ref()) == Runlog()


###
### test AddParameters wrapper
###


def test_pipeline_run_parameters():
    task1 = Task(AddParameters(first_parameter=42, another_parameter="foobar")(id_tf))
    task2 = Task(
        compose(
            AddParameters(first_parameter=42),
            AddParameters(another_parameter="foobar"),
        )(id_tf)
    )

    for task in [task1, task2]:
        task.start(Future.value(Runlog()))

        assert ray.get(task.get_ref()) == Runlog(
            first_parameter=42, another_parameter="foobar"
        )


def test_pipeline_dynamic_run_parameter():
    task = Task(
        compose(
            AddDynamicParameter("z", lambda runlog: runlog["y"] + 1),
            AddDynamicParameter("y", lambda runlog: runlog["x"] + 1),
            AddParameters(x=42),
        )(id_tf)
    )

    task.start(Future.value(Runlog()))

    assert ray.get(task.get_ref()) == Runlog(x=42, y=43, z=44)
