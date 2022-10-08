import pynb_dag_runner


def test_version_info():
    assert pynb_dag_runner.__version__ == "0.0.0+local"
    assert pynb_dag_runner.__git_sha__ == 40 * "0"
