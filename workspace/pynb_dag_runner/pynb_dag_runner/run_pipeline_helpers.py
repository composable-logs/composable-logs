import os
from typing import List, Mapping, Optional, TypeVar


K = TypeVar("K")
V = TypeVar("V")


def _dict_filter_none_values(a_dict: Mapping[K, Optional[V]]) -> Mapping[K, V]:
    return {k: v for k, v in a_dict.items() if v is not None}


def get_github_env_variables() -> Mapping[str, str]:
    """
    Returns a Python dict with key-values representing Github and Github actions
    details. These are set as environment variables when code is run run as a Github
    action task.

    The selected list of environment variables being read is listed below.

    The full list of available variables (with documentation):
    https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables

    Notes:
    - If no Github-environment variables are set (eg. code is run locally), this
      function returns an empty dictionary. (Ie., keys with none values are removed
      from output).

    - In the returned dictionary, variables are placed in lower case and prefixed with
      pipeline.github. For example, the key "pipeline.github.actor" contains the
      Github username who triggered the task run.
    """
    gh_env_vars: List[str] = [
        #
        # ===== repository level metadata =====
        #
        "GITHUB_REPOSITORY",
        # eg. "pynb-dag-runner/mnist-digits-demo-pipeline"
        #
        #
        # ===== gha job level metadata =====
        #
        "GITHUB_WORKFLOW",
        # human readable description of gha job
        #
        "RUNNER_NAME",
        # where was task run. eg "Hosted Agent"
        #
        "GITHUB_RUN_ID",
        # eg "nnnnnnnnnnnn". This can be used to link back to job run page on Github
        #
        "GITHUB_ACTOR",
        # user name triggering task.
        # See also:
        #   https://github.community/t/who-will-be-the-github-actor-when-a-workflow-runs-on-a-schedule/17369
        #
        "GITHUB_JOB",
        # name of gha job
        #
        #
        # ===== Metadata for runs triggered by a pull request =====
        #
        "GITHUB_BASE_REF",
        # target branch for pull request, eg., "main"
        #
        "GITHUB_HEAD_REF",
        # eg. feature branch name for pull request
        #
        #
        # ===== git metadata =====
        #
        "GITHUB_SHA",
        # sha of commit being run
        #
        "GITHUB_REF",
        # see above link for formats, eg., "refs/pull/nn/merge"
        #
        "GITHUB_REF_TYPE",
        # possible values "branch" or "tag"
        #
        "GITHUB_REF_NAME",
        # eg "40/merge"
        #
        "GITHUB_EVENT_NAME",
        # eg "pull_request" or "workflow_dispatch"
        #
    ]

    return _dict_filter_none_values(
        {
            "pipeline.github." + var.lower().replace("github_", ""): os.getenv(var)
            for var in gh_env_vars
        }
    )
