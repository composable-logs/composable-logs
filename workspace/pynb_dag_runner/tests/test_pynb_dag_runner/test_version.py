import pynb_dag_runner


def _is_valid(s: str) -> bool:
    return isinstance(s, str) and len(s) > 0


def test_version_info():
    assert _is_valid(pynb_dag_runner.__version__)
    assert _is_valid(pynb_dag_runner.__git_sha__)
