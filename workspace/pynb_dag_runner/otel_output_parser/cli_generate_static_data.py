import io, json
from zipfile import ZipFile

from pathlib import Path
from functools import lru_cache
from argparse import ArgumentParser

# -
from pynb_dag_runner import version_string
from pynb_dag_runner.helpers import dict_prefix_keys
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import (
    parse_spans,
    ArtifactContent,
)

# -
from .mermaid_graphs import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
)
from .common_helpers.github_helpers import github_repo_artifact_zips
from .common_helpers.utils import ensure_dir_exist


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
        "--output_www_root_directory",
        required=True,
        type=Path,
        help="Output www-root directory (to write metadata json and artifacts)",
    )
    return parser.parse_args()


def get_recorded_spans_from_zip(zip_file_content: bytes) -> Spans:
    """
    Input:
       zip_file_content: bytes
            byte-array with zip file created by workflow run process with recorded
            OpenTelemetry spans recorded in a "opentelemetry-spans.json" file.

    Output:
       Spans object with parsed JSON

    """
    # convert byte-array into something that can be opened as a file
    z_io: io.BytesIO = io.BytesIO(zip_file_content)

    with ZipFile(z_io, "r") as z:  # type: ignore
        return Spans(json.loads(z.read("opentelemetry-spans.json")))


def _write_artifacts(output_path: Path, artifacts):
    for artifact in artifacts:
        artifact.write(ensure_dir_exist(output_path / artifact.name))

    return artifacts


def process(spans: Spans, www_root: Path):
    workflow_summary = parse_spans(spans)
    print("> processing workflow run", workflow_summary.span_id)

    workflow_artifact_relative_root = (
        Path("artifacts") / "workflow" / workflow_summary.span_id
    )

    # The below are not real artifacts logged during workflow execution.
    # Normal artifacts (and logged values) are only logged on per-task level
    # (and not on workflow-level).
    #
    # Rather, these are assets generated for reporting/visualisation.
    #
    # We want to generate these *after the workflow has run* so these
    # can be generated dynamically and updated independently without having
    # to rerun the workflow.
    #
    reporting_artifacts = [
        ArtifactContent(
            name="dag.mmd",
            type="utf-8",
            content=make_mermaid_dag_inputfile(spans, generate_links=True),
        ),
        ArtifactContent(
            name="dag-nolinks.mmd",
            type="utf-8",
            content=make_mermaid_dag_inputfile(spans, generate_links=False),
        ),
        ArtifactContent(
            name="gantt.mmd",
            type="utf-8",
            content=make_mermaid_gantt_inputfile(spans),
        ),
        ArtifactContent(
            name="run-time-metadata.json",
            type="utf-8",
            content=json.dumps(workflow_summary.as_dict(), indent=2),
        ),
    ]

    # TODO: The below dict:s are optimised for UI/reporting. They are slightly
    # different from the generic summary dict:s created when parsing workflow
    # runtime logs.
    yield {
        "parent_span_id": None,
        "span_id": workflow_summary.span_id,
        "type": "workflow",
        **dict_prefix_keys("timing_", workflow_summary.timing.as_dict()),
        "is_success": workflow_summary.is_success(),
        "attributes": workflow_summary.attributes,
        "artifacts": [
            artifact.metadata_as_dict()
            for artifact in _write_artifacts(
                output_path=www_root / workflow_artifact_relative_root,
                artifacts=reporting_artifacts,
            )
        ],
    }

    for task_run_summary in workflow_summary.task_runs:
        task_artifact_relative_root = (
            Path("artifacts") / "task" / task_run_summary.span_id
        )
        print(" - task", task_artifact_relative_root)
        yield {
            "parent_span_id": workflow_summary.span_id,
            "span_id": task_run_summary.span_id,
            "type": "task",
            "task_id": task_run_summary.task_id,
            **dict_prefix_keys("timing_", task_run_summary.timing.as_dict()),
            "is_success": task_run_summary.is_success(),
            # List of exceptions are not included in the reporting JSON.
            # TODO: write to an artifact for inspection.
            "attributes": task_run_summary.attributes,
            # note: In addition to "logged_artifacts" the below also include metadata
            # for additional artifacts generated for reporting (like mermaid diagrams).
            "artifacts": [
                artifact.metadata_as_dict()
                for artifact in _write_artifacts(
                    output_path=www_root / task_artifact_relative_root,
                    artifacts=(
                        task_run_summary.logged_artifacts
                        # log artifacts logged during task run. in addition, log
                        # a json with metadata logged *during run time*
                        + [
                            ArtifactContent(
                                name="run-time-metadata.json",
                                type="utf-8",
                                content=json.dumps(
                                    task_run_summary.as_dict(), indent=2
                                ),
                            )
                        ]
                    ),
                )
            ],
            "logged_values": {
                k: v.as_dict() for k, v in task_run_summary.logged_values.items()
            },
        }


def entry_point():
    print(f"--- generate_static_data cli (v. {version_string()})")
    print(" - github_repository             :", args().github_repository)
    print(" - zip_cache_dir                 :", args().zip_cache_dir)
    print(" - output_www_root_directory     :", args().output_www_root_directory)

    entries = []
    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):
        spans = get_recorded_spans_from_zip(artifact_zip)
        print(f"--- Processing new zip with {len(spans)} spans ...")

        for entry in process(spans, args().output_www_root_directory):
            entries.append(entry)

    (
        ensure_dir_exist(args().output_www_root_directory / "static_data.json")
        # -
        .write_text(json.dumps(entries, indent=2))
    )

    print("Done")
