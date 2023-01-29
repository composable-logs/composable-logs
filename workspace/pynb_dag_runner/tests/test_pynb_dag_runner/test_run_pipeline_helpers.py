import pytest

# -
from pynb_dag_runner.run_pipeline_helpers import get_github_env_variables


@pytest.fixture
def github_env(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTOR", "someone")
    # Note: unlike other variables, RUNNER_NAME doesn't have a "GITHUB"-prefix.
    monkeypatch.setenv("RUNNER_NAME", "my-runner-vm-2")

    try:
        monkeypatch.delenv("GITHUB_SHA")
    except:
        pass


def test_get_github_env_variables(github_env):
    result = get_github_env_variables()

    assert result["workflow.github.actor"] == "someone"
    assert result["workflow.github.runner_name"] == "my-runner-vm-2"

    assert "GITHUB_SHA" not in result.keys()
