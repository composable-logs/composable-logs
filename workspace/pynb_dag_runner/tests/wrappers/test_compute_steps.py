import time
from pathlib import Path
from typing import Awaitable

#
import ray, pytest

#
from pynb_dag_runner.helpers import compose, read_json
from pynb_dag_runner.ray_helpers import Future
from pynb_dag_runner.core.dag_runner import Task
from pynb_dag_runner.wrappers.runlog import Runlog
from pynb_dag_runner.wrappers.compute_steps import (
    T,
    AddParameters,
    AddDynamicParameter,
    AddPythonFunctionCall,
    AddTiming,
    AddRetries,
    AddPersistRunlog,
    AddCreateRunlogOutputPath,
)


# define identity transformation Future[Runlog] -> Future[Runlog]
id_tf: T[Awaitable[Runlog]] = Future.lift(lambda runlog: runlog)


def test_pipeline_no_compute_steps():
    # check expected output for task that does nothing to runlog

    task1 = Task(f_remote=id_tf)
    task1.start(Future.value(Runlog()))

    task2 = Task(f_remote=lambda: Future.value(Runlog()))
    task2.start()

    for task in [task1, task2]:
        assert ray.get(task.get_ref()) == Runlog()


###
### ---- AddParameters tests ----
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


###
### ---- AddParameters tests ----
###


def test_pipeline_add_timing_wrapper():
    def f_wait(_: Runlog):
        time.sleep(0.3)

    task = Task(compose(AddTiming(), AddPythonFunctionCall(f_wait))(id_tf))
    task.start(Future.value(Runlog()))

    result_runlog = ray.get(task.get_ref())
    assert result_runlog.keys() == set(
        [
            # keys added by AddPythonFunctionCall-wrapper
            "out.error",
            "out.status",
            "out.result",
            "parameters.task.timeout_s",
            # keys added by AddTiming-wrapper
            "out.timing.start_ts",
            "out.timing.end_ts",
            "out.timing.duration_ms",
        ]
    )

    # Timing can be ~2000ms on 2 core Github runner.
    # Thus, duration_ms is not currently not very precise. This could be due to lots of
    # ray.remote-functions (that each may have overhead?). Or, maybe Ray startup?
    assert result_runlog["out.timing.duration_ms"] in range(300, 4000)


###
### ---- AddRetries tests ----
###


def test_pipeline_add_retries():
    # Note: this test becomes slow for larger retry counts
    n_max_retries: int = 3

    def f_crash_but_succeed_on_last_retry(runlog: Runlog):
        assert "parameters.run.retry_nr" in runlog.keys()
        assert "parameters.task.n_max_retries" in runlog.keys()

        if runlog["parameters.run.retry_nr"] == n_max_retries - 1:
            return 123

        raise Exception("BOOM!")

    task = Task(
        compose(
            AddRetries(n_max_retries=n_max_retries),
            AddPythonFunctionCall(f_crash_but_succeed_on_last_retry),
        )(id_tf)
    )
    task.start(Future.value(Runlog()))

    assert ray.get(task.get_ref()) == [
        Runlog(
            **{
                "parameters.task.timeout_s": None,
                "out.status": "FAILURE",
                "out.error": "BOOM!",
                "out.result": None,
                "parameters.run.retry_nr": i,
                "parameters.task.n_max_retries": n_max_retries,
            },
        )
        for i in range(n_max_retries - 1)
    ] + [
        Runlog(
            **{
                "parameters.task.timeout_s": None,
                "out.status": "SUCCESS",
                "out.error": None,
                "out.result": 123,
                "parameters.run.retry_nr": n_max_retries - 1,
                "parameters.task.n_max_retries": n_max_retries,
            },
        )
    ]


###
### ---- Persist runlog wrapper tests ----
###


@pytest.mark.parametrize("should_succeed", [True, False])
def test_pipeline_persist_runlog(tmp_path: Path, should_succeed: bool):
    def f(runlog: Runlog):
        if should_succeed:
            (Path(runlog["path"]) / "some-logged-artefacts").touch()
            return 10
        else:
            raise Exception("BOOM!")

    task = Task(
        compose(
            AddPersistRunlog(),
            AddPythonFunctionCall(f),
            AddCreateRunlogOutputPath(get_run_path=lambda runlog: runlog["path"]),
            AddParameters(path=str(tmp_path)),  # type: ignore
        )(id_tf)
    )
    task.start(Future.value(Runlog()))

    _ = ray.get(task.get_ref())

    assert (tmp_path / "_SUCCESS").is_file() == should_succeed
    assert (tmp_path / "some-logged-artefacts").is_file() == should_succeed

    persisted_runlog = read_json(tmp_path / "runlog.json")
    if should_succeed:
        assert persisted_runlog == {
            "path": str(tmp_path),
            "parameters.task.timeout_s": None,
            "parameters.run.run_directory": str(tmp_path),
            "out.status": "SUCCESS",
            "out.error": None,
            "out.result": 10,
        }
    else:
        assert persisted_runlog == {
            "path": str(tmp_path),
            "parameters.task.timeout_s": None,
            "parameters.run.run_directory": str(tmp_path),
            "out.status": "FAILURE",
            "out.error": "BOOM!",
            "out.result": None,
        }
