from typing import Any
from pathlib import Path
from argparse import ArgumentParser

from .github_helpers import list_artifacts_for_repo, download_artifact
from .static_builder import process_artifact

# `static_builder` command line utility --- run `statuc_builder --help` for usage


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--gh_repo_owner",
        required=True,
        type=str,
        help="Github repository owner (organization or user name)",
    )
    parser.add_argument(
        "--gh_repo_name",
        required=True,
        type=str,
        help="Github repo name with artefacts to download",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        type=str,
        help="Output directory",
    )
    return parser.parse_args()


def entry_point():
    gh_repo_owner = args().gh_repo_owner
    gh_repo_name = args().gh_repo_name
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
