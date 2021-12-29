import time

#
import ray, pytest

#
from pynb_dag_runner.helpers import one
from pynb_dag_runner.core.dag_runner import RemoteTaskP, task_from_python_function
from pynb_dag_runner.opentelemetry_helpers import (
    get_span_exceptions,
    Span,
    Spans,
    SpanRecorder,
)


@pytest.mark.parametrize("task_fail", [True, False])
def test_make_task_from_function_or_remote_function(task_fail: bool):
    def f(_):
        time.sleep(0.125)
        if task_fail:
            raise Exception("kaboom!")
        else:
            return 1234

    def get_test_spans() -> Spans:
        with SpanRecorder() as sr:
            task: RemoteTaskP = task_from_python_function(
                f, tags={"foo": "my_test_func"}
            )

            assert ray.get(task.has_started.remote()) == False
            assert ray.get(task.has_completed.remote()) == False

            for _ in range(10):
                task.start.remote(None)

            assert ray.get(task.has_completed.remote()) == False
            assert ray.get(task.has_started.remote()) == True

            result = ray.get(task.get_task_result.remote())
            if task_fail:
                assert result.return_value is None
                assert "kaboom!" in str(result.error)
            else:
                assert result.return_value == 1234
                assert result.error is None

            assert ray.get(task.has_started.remote()) == True
            assert ray.get(task.has_completed.remote()) == True
        return sr.spans

    def validate_spans(spans: Spans):
        # there should be dependencies logged with only one task
        assert len(spans.filter(["name"], "task-dependency")) == 0

        def get_span_for_task(func_name: str) -> Span:
            assert func_name in ["f", "g", "h"]
            return one(spans.filter(["attributes", "tags.foo"], "my_test_func"))

        # check exceptions
        task_span = get_span_for_task("g")
        if task_fail:
            assert task_span["status"] == {
                "status_code": "ERROR",
                "description": "Remote function call failed",
            }
            exception = one(get_span_exceptions(task_span))["attributes"]
            assert exception["exception.type"] == "RayTaskError"
            assert "kaboom!" in exception["exception.message"]
        else:
            assert len(spans.exceptions_in(task_span)) == 0
            assert task_span["status"] == {"status_code": "OK"}

    validate_spans(get_test_spans())
