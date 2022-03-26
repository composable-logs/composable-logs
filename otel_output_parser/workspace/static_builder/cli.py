from typing import Any, Iterable, Optional
from pathlib import Path
from argparse import ArgumentParser

from .github_helpers import list_artifacts_for_repo, download_artifact
from .static_builder import process_artifact, ensure_dir_exist


# Run as:
# $ pip install -e .
#
# Github token should have public repo scope (for personal access token)
# $ export GITHUB_TOKEN="..."
# $ static_builder --github_repository pynb-dag-runner/mnist-digits-demo-pipeline --output_dir ./output
#


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--github_repository",
        required=False,
        type=str,
        help="Github repo owner and name. eg. myorg/myrepo",
    )
    parser.add_argument(
        "--zip_cache_dir",
        required=False,
        type=Path,
        help="Directory with cached zip artefacts (or directory where to write zips)",
    )
    parser.add_argument(
        "--output_dir",
        required=False,
        type=Path,
        help="Output directory",
    )
    return parser.parse_args()


def github_repo_artifact_zips(
    github_repository: Optional[str], zip_cache_dir: Optional[Path]
) -> Iterable[bytes]:
    """
    Arguments:
     - `github_repository` reference to Github repo in format owner/repo-name
     - `zip_cache_dir` local directory for caching artifacts.

    At least one argument should be set (ie. not None).

    ------------------+---------------+-------------------------------------------------
    github_repository | zip_cache_dir | Returns
    ------------------+---------------+-------------------------------------------------
    None              | None          | Not valid
    ------------------+---------------+-------------------------------------------------
    Set               | None          | Return iterator with all artifacts fetched from
                      |               | a Github repo.
    ------------------+---------------+-------------------------------------------------
    Set               | Set           | Return iterator with all artifacts from the
                      |               | Github repo, and write the artifacts to cache
                      |               | directory.
    ------------------+---------------+-------------------------------------------------
    None              | Set           | Return iterator with zip artifacts in cache
                      |               | directory.
    ------------------+---------------+-------------------------------------------------

    """

    if github_repository is not None:
        if len(github_repository.split("/")) != 2:
            raise ValueError(
                "github_repository parameter should be in format owner/repo-name"
            )

        gh_repo_owner, gh_repo_name = github_repository.split("/")
        print("Fetching artefacts from Github")
        print(" - repo_owner   :", gh_repo_owner)
        print(" - repo_name    :", gh_repo_name)

        # fetch artifacts from Github, and possibly cache them to local directory
        for entry in list_artifacts_for_repo(
            repo_owner=gh_repo_owner, repo_name=gh_repo_name
        ):
            if entry["expired"]:
                continue

            artifact_id: str = str(entry["id"])
            artifact_zip: bytes = download_artifact(
                repo_owner=gh_repo_owner,
                repo_name=gh_repo_name,
                artifact_id=artifact_id,
            )

            if zip_cache_dir is not None:
                cache_file: Path = zip_cache_dir / (artifact_id + ".zip")
                print(f" - Caching {cache_file} ({len(artifact_zip)} bytes) ...")
                ensure_dir_exist(Path(cache_file)).write_bytes(artifact_zip)

            yield artifact_zip

    elif zip_cache_dir is not None and github_repository is None:
        # use local cache; no requests to Github
        for f in zip_cache_dir.glob("*.zip"):
            print(f, type(f))
            yield Path(f).read_bytes()

    else:
        assert github_repository is None and zip_cache_dir is None
        raise ValueError("Both github_repository and zip_cache_dir can not be None")


def entry_point():
    print("output_dir         :", args().output_dir)
    print("github_repository  :", args().github_repository)
    print("zip_cache_dir      :", args().zip_cache_dir)

    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):

        process_artifact(artifact_zip, output_dir=args().output_dir)

    print("Done")
