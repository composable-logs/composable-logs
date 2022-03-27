from typing import Any, Iterable, Optional
from pathlib import Path
from functools import lru_cache
from argparse import ArgumentParser

from .github_helpers import list_artifacts_for_repo, download_artifact
from .static_builder import linearize_log_events, ensure_dir_exist

"""
Run as:
$ pip install -e .

Set Github token (should have public repo scope, for personal access token):
$ export GITHUB_TOKEN="..."

Download artifact into cache directory, parse into output directory:
$ static_builder --zip_cache_dir ./cache --github_repository pynb-dag-runner/mnist-digits-demo-pipeline --output_dir ./output
"""


@lru_cache
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
        required=True,
        type=Path,
        help="Output directory for parsed content (json:s and logged artifacts)",
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
            if entry["expired"]:
                continue

            artifact_id: str = str(entry["id"])
            artifact_zip: bytes = download_artifact(
                github_repository=github_repository, artifact_id=artifact_id
            )

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


def entry_point():
    print("output_dir         :", args().output_dir)
    print("github_repository  :", args().github_repository)
    print("zip_cache_dir      :", args().zip_cache_dir)

    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):
        linear_events = linearize_log_events(artifact_zip)

        for run_summary in linear_events:
            for art in run_summary["artifacts"]:
                print(f"- writing {art['artifact_path']} ({len(art['content'])} bytes)")

                ensure_dir_exist(args().output_dir / art["artifact_path"]).write_bytes(
                    art["content"]
                )

    print("Done")
