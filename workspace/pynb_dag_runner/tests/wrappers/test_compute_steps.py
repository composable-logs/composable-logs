import time

#
import ray

#
from pynb_dag_runner.helpers import compose
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.core.dag_runner import Task
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.wrappers.compute_steps import (
    T,
    AddParameters,
    AddDynamicParameter,
    AddPythonFunctionCall,
)


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


###
### ---- AddPythonFunctionCall tests ----
###


def test_add_python_function_call_success():
    for timeout_s in [None, 10, 20]:

        def f(runlog: Runlog):
            # The timeout_s key should be set before function is called.
            assert "parameters.task.timeout_s" in runlog.keys()
            return 123

        task = Task(AddPythonFunctionCall(f, timeout_s=timeout_s)(id_tf))
        task.start(Future.value(Runlog()))

        assert ray.get(task.get_ref()) == Runlog(
            **{
                "parameters.task.timeout_s": timeout_s,
                "out.status": "SUCCESS",
                "out.error": None,
                "out.result": 123,
            },
        )


def test_add_python_function_call_exception_handling():
    def failing_function(runlog: Runlog):
        raise Exception("BOOM!")

    task = Task(AddPythonFunctionCall(failing_function)(id_tf))
    task.start(Future.value(Runlog()))

    assert ray.get(task.get_ref()) == Runlog(
        **{
            "parameters.task.timeout_s": None,
            "out.status": "FAILURE",
            "out.error": "BOOM!",
            "out.result": None,
        },
    )


def test_add_python_function_call_timeout():
    def f_fail(_: Runlog):
        time.sleep(1)
        raise Exception("BOOM!")

    def f_hanging(_: Runlog):
        time.sleep(1e6)

    for f in [f_fail, f_hanging]:
        task = Task(AddPythonFunctionCall(f, timeout_s=0.01)(id_tf))
        task.start(Future.value(Runlog()))

        assert ray.get(task.get_ref()) == Runlog(
            **{
                "parameters.task.timeout_s": 0.01,
                "out.status": "FAILURE",
                "out.error": "Timeout error: execution did not finish within timeout limit",
                "out.result": None,
            },
        )


def test_add_python_function_call_func_can_depend_on_runlog_content():
    def f(runlog: Runlog):
        assert runlog.as_dict() == {
            "foo": 42,
            "parameters.task.timeout_s": None,
        }
        return runlog["foo"] + 1

    task = Task(compose(AddPythonFunctionCall(f), AddParameters(foo=42))(id_tf))
    task.start(Future.value(Runlog()))

    assert ray.get(task.get_ref()) == Runlog(
        **{
            "foo": 42,
            "parameters.task.timeout_s": None,
            "out.status": "SUCCESS",
            "out.error": None,
            "out.result": 43,
        },
    )
