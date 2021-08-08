# location of this file indicates root for pytest
import pytest, ray


@pytest.fixture(scope="session", autouse=True)
def init_ray_before_all_tests():
    # - Init ray once before all tests.
    # - This way there is no need for ray.init commands in the in test python files.
    #   Hence ray do not need to start when VS Code is discovering tests (which seems
    #   to generate errors).
    ray.init(num_cpus=2)
