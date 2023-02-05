import composable_logs


def _is_valid(s: str) -> bool:
    return isinstance(s, str) and len(s) > 0


def test_version_info():
    assert _is_valid(composable_logs.__version__)
    assert _is_valid(composable_logs.__git_sha__)
