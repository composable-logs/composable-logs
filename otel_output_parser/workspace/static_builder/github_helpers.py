import os
from typing import Dict, List, Iterable

#

# For full list of ghapi methods, see https://ghapi.fast.ai/fullapi.html
from ghapi.all import GhApi  # type: ignore
import requests  # type: ignore


def _paginator(operation, per_page=30, **kwargs) -> Iterable[Dict]:
    """
    Paginator-wrapper suitable for getting all results from list_artifacts_for_repo api.

    The GhApi library has built in pagainator-wrapper (from ghapi.page import paged).
    This is described here: https://ghapi.fast.ai/page.html

    However, this wrapper seems to loop over a fixed number of pages (default 9999)
    even when there is less data. This issue is known, see
    https://github.com/fastai/ghapi/issues/96  which also includes a workaround.
    However, this does not seem to work for list_artifacts_for_repo since
    "incomplete_results" is not a key in results returned from this api.
    """
    count = 0
    for page in range(1, 9999):
        result = operation(**kwargs, per_page=per_page, page=page)
        # assert "incomplete_results" not in result

        if len(result["artifacts"]) == 0:
            break
        for entry in result["artifacts"]:
            yield dict(entry)
            count += 1

    assert count == result["total_count"]


def list_artifacts_for_repo(repo_owner: str, repo_name: str) -> List[Dict]:
    """
    List all artefacts in a Github repo.

    Environment variable GITHUB_TOKEN should contain valid token (either token
    generated for an action run, or a Github personal access token).

    The required scope for the token is documented here:
    https://docs.github.com/en/rest/reference/actions#artifacts

    See above link for strucuture of return values.
    """
    api = GhApi()

    return list(
        _paginator(
            api.actions.list_artifacts_for_repo, owner=repo_owner, repo=repo_name
        )
    )


def download_artifact(repo_owner: str, repo_name: str, artifact_id: str) -> bytes:
    """
    Download artifact from Github repo

    API Documentation
    https://docs.github.com/en/rest/reference/actions#download-an-artifact

    Note:
     - download artifact api did not seem to work with GhApi library
    """
    token = os.getenv("GITHUB_TOKEN")
    if token is None:
        raise Exception("GITHUB_TOKEN should be set")

    endpoint = "https://api.github.com/repos/"
    url = f"{repo_owner}/{repo_name}/actions/artifacts/{artifact_id}/zip"
    response = requests.get(
        endpoint + url,
        headers={"authorization": f"Bearer {token}"},
        allow_redirects=True,
    )
    if response.status_code != requests.codes.ok:
        raise Exception(f"Request failed with code {response.status_code}")

    return response.content
