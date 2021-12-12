import time

#
import ray

#
from pynb_dag_runner.core.dag_runner import task_from_func, task_from_remote_f


def test__task_ot__make_task_from_remote_function__success():
    @ray.remote(num_cpus=0)
    def f():
        time.sleep(0.125)
        return 1234

    def g():
        time.sleep(0.125)
        return 1234

    for task in [task_from_remote_f(f.remote), task_from_func(g)]:
        assert ray.get(task.has_started.remote()) == False
        assert ray.get(task.has_completed.remote()) == False

        for _ in range(4):
            task.start.remote()

        assert ray.get(task.has_completed.remote()) == False
        assert ray.get(task.has_started.remote()) == True

        result = ray.get(task.get_result.remote())
        assert result.return_value == 1234
        assert result.error is None

        assert ray.get(task.has_started.remote()) == True
        assert ray.get(task.has_completed.remote()) == True


def test__task_ot__make_task_from_function__fail():
    def f():
        time.sleep(1)
        raise Exception("kaboom!")

    task_f = task_from_func(f)

    assert ray.get(task_f.has_started.remote()) == False
    assert ray.get(task_f.has_completed.remote()) == False

    for _ in range(1000):
        task_f.start.remote()

    assert ray.get(task_f.has_started.remote()) == True
    assert ray.get(task_f.has_completed.remote()) == False

    result = ray.get(task_f.get_result.remote())

    assert ray.get(task_f.has_started.remote()) == True
    assert ray.get(task_f.has_completed.remote()) == True

    assert result.return_value is None
    assert "kaboom!" in str(result.error)
