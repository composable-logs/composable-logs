from typing import Any
from pathlib import Path
from argparse import ArgumentParser

from .github_helpers import list_artifacts_for_repo, download_artifact
from .static_builder import process_artifact

# `static_builder` command line utility --- run `statuc_builder --help` for usage


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--github_repository",
        required=True,
        type=str,
        help="Github repo owner and name. eg. myorg/myrepo",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        type=str,
        help="Output directory",
    )
    return parser.parse_args()


def entry_point():

    github_repository = args().github_repository
    if len(github_repository.split("/")) != 2:
        raise ValueError("github_repository parameter should be in format owner/repo-name")

    gh_repo_owner, gh_repo_name = github_repository.split("/")

    output_dir = args().output_dir

    print("gh_repo_owner   :", gh_repo_owner)
    print("gh_repo_name    :", gh_repo_name)
    print("output_dir      :", output_dir)

    results = list_artifacts_for_repo(repo_owner=gh_repo_owner, repo_name=gh_repo_name)
    for idx, entry in enumerate(results):
        if entry["expired"]:
            continue
        print(idx, entry)

        artefact_zip = download_artifact(
            repo_owner=gh_repo_owner,
            repo_name=gh_repo_name,
            artifact_id=entry["id"],
        )

        process_artifact(artefact_zip, output_dir=Path(output_dir))
