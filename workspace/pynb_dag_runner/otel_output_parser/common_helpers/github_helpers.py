import os
from pathlib import Path
from typing import Dict, List, Iterable, Optional

#

# For full list of ghapi methods, see https://ghapi.fast.ai/fullapi.html
from ghapi.all import GhApi  # type: ignore
import requests  # type: ignore

#
from .utils import ensure_dir_exist


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


def _validate_github_repo_setup(github_repository: str):
    if len(github_repository.split("/")) != 2:
        raise ValueError(
            "github_repository parameter should be in format owner/repo-name"
        )

    if os.getenv("GITHUB_TOKEN") is None:
        raise Exception("GITHUB_TOKEN should be set")


def list_artifacts_for_repo(github_repository: str) -> List[Dict]:
    """
    List all artefacts in a Github repo (in format owner/repo-name).

    Environment variable GITHUB_TOKEN should contain valid token (either token
    generated for an action run, or a Github personal access token).

    The required scope for the token is documented here:
    https://docs.github.com/en/rest/reference/actions#artifacts

    See above link for strucuture of return values (a list of Python dict:s).
    """
    _validate_github_repo_setup(github_repository)

    api = GhApi()
    repo_owner, repo_name = github_repository.split("/")
    return list(
        _paginator(
            api.actions.list_artifacts_for_repo, owner=repo_owner, repo=repo_name
        )
    )


def download_artifact(github_repository: str, artifact_id: str) -> Optional[bytes]:
    """
    Download artifact from Github repo

    API Documentation
    https://docs.github.com/en/rest/reference/actions#download-an-artifact

    Note:
     - download artifact api did not seem to work with GhApi library
    """
    _validate_github_repo_setup(github_repository)

    token = os.getenv("GITHUB_TOKEN")
    endpoint = "https://api.github.com/repos/"
    url = f"{github_repository}/actions/artifacts/{artifact_id}/zip"
    response = requests.get(
        endpoint + url,
        headers={"authorization": f"Bearer {token}"},
        allow_redirects=True,
    )
    if response.status_code == 410:
        print(
            " - Got error code 410 (Gone): Could the content "
            "have expired after downloading list of artifacts?"
        )
        return None

    if response.status_code != requests.codes.ok:
        raise Exception(f"Request failed with code {response.status_code}")

    return response.content


def github_repo_artifact_zips(
    github_repository: Optional[str], zip_cache_dir: Optional[Path]
) -> Iterable[bytes]:
    """
    Arguments:
     - `github_repository` reference to Github repo in format owner/repo-name
     - `zip_cache_dir` local directory for caching artifacts.

    At least one argument should be set (ie. not None).

    Input parameter combinations and actions:

    1) github_repository=None, zip_cache_dir=None
       Not possible

    2) github_repository=Set, zip_cache_dir=None
       Return iterator with all zip artifacts fetched from the Github repo.

    3) github_repository=None, zip_cache_dir set
       Return iterator with all zip artifacts fetched from the cache directory.

    3) github_repository set, zip_cache_dir set
       Return iterator with all zip artifacts from the Github repo, and also write
       each zip artifacts to cache directory.
    """

    if github_repository is not None:
        # fetch artifacts from Github, and possibly cache them to local directory

        print("Fetching artefacts from Github : ", github_repository)
        for entry in list_artifacts_for_repo(github_repository=github_repository):
            if entry["expired"] or ("opentelemetry-outputs-v1" not in entry["name"]):
                continue

            artifact_id: str = str(entry["id"])
            artifact_zip: Optional[bytes] = download_artifact(
                github_repository=github_repository, artifact_id=artifact_id
            )

            if artifact_zip is None:
                continue  # artifact could have expired after list was created?

            if zip_cache_dir is not None:
                cache_file: Path = zip_cache_dir / (artifact_id + ".zip")
                print(f" - Caching {cache_file} ({len(artifact_zip)} bytes) ...")
                ensure_dir_exist(Path(cache_file)).write_bytes(artifact_zip)

            yield artifact_zip

    elif zip_cache_dir is not None and github_repository is None:
        # use local cache; no requests to Github
        for f in zip_cache_dir.glob("*.zip"):
            yield f.read_bytes()

    else:
        assert github_repository is None and zip_cache_dir is None
        raise ValueError("Both github_repository and zip_cache_dir can not be None")
