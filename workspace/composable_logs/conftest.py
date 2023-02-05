import shutil

# location of this file (conftest.py) indicates root for pytest
import pytest, ray


def _ray_init():
    # - Init ray once before all tests.
    # - This way there is no need for ray.init commands in the in test python files.
    #   Hence ray do not need to start when VS Code is discovering tests (which seems
    #   to generate errors).
    ray.init(
        # By giving explicit namespace to cluster we can connect by name
        # eg. from notebooks
        namespace="pydar-ray-cluster",
        num_cpus=2,
        ignore_reinit_error=True,
        # enable tracing and write traces to /tmp/spans/<pid>.txt in JSONL format
        _tracing_startup_hook="ray.util.tracing.setup_local_tmp_tracing:setup_tracing",
    )


@pytest.fixture(scope="session", autouse=True)
def init_ray_before_all_tests():
    # Clean up any spans from previous runs
    shutil.rmtree("/tmp/spans", ignore_errors=True)
    ray.shutdown()
    _ray_init()
    yield
    ray.shutdown()


@pytest.fixture
def ray_reinit():
    # This fixture allows us to reset cluster before individual (flaky) tests
    ray.shutdown()
    _ray_init()
    yield
    ray.shutdown()
    _ray_init()
