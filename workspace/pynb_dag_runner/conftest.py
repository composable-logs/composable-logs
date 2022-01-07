import gc, shutil

# location of this file (conftest.py) indicates root for pytest
import pytest, ray


def _ray_init():
    """
    - Init ray for running tests.
    - This way there is no need for ray.init commands in the in test python files.
      Hence ray do not need to start when VS Code is discovering tests (which seems
      to generate errors).
    """
    ray.init(
        num_cpus=2,
        # enable tracing and write traces to /tmp/spans/<pid>.txt in JSONL format
        _tracing_startup_hook="ray.util.tracing.setup_local_tmp_tracing:setup_tracing",
    )


@pytest.fixture(scope="session", autouse=True)
def init_ray_before_all_tests():
    # Clean up any spans from previous runs
    shutil.rmtree("/tmp/spans", ignore_errors=True)

    _ray_init()
    yield
    ray.shutdown()


_NR_RAY = 0


@pytest.fixture(scope="function", autouse=True)
def func_wrapper():
    global _NR_RAY

    _NR_RAY += 1

    if _NR_RAY % 10 == 0:
        # reinit Ray on every 10th tests
        ray.shutdown()
        _ray_init()
        yield
        ray.shutdown()


# @pytest.fixture(scope="module", autouse=True)
# def module_wrapper():
#     ray.shutdown()
#     _ray_init()
#     yield
#     ray.shutdown()
