import io, json
from zipfile import ZipFile
from typing import Any, Iterable

from pathlib import Path
from functools import lru_cache
from argparse import ArgumentParser

# -
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
    add_html_notebook_artefacts,
)
from .mermaid_graphs import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
)
from .static_builder.cli import write_attachment_sink

# -
from .common_helpers.github_helpers import github_repo_artifact_zips


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
        required=False,
        type=Path,
        help="Output www-root directory (to write metadata json and artifacts)",
    )
    return parser.parse_args()


def get_recorded_spans_from_zip(zip_file_content: bytes) -> Spans:
    """
    Input:
       zip_file_content: bytes
            byte-array with zip file created by pipeline run process with recorded
            OpenTelemetry spans recorded in a "opentelemetry-spans.json" file.

    Output:
       Spans object with parsed JSON

    """
    # convert byte-array into something that can be opened as a file
    z_io: io.BytesIO = io.BytesIO(zip_file_content)

    with ZipFile(z_io, "r") as z:  # type: ignore
        return Spans(json.loads(z.read("opentelemetry-spans.json")))


def linearize_log_events(spans: Spans) -> Iterable[Any]:
    """
    Linearize log events in zip artifact into iterator of dicts with keys:

     - type ("pipeline", "task", "run")
     - id: (uuid or otel span id as string)
     - parent_id:
        - None (when type=pipeline)
        - parent pipeline's id (when type=task)
        - parent task's id (when type=run)
     - metadata: (pipeline.json, task.json, or run.json depending on type)
     - artifacts: list of dicts with "file_name", "artifact_path, "content" (see below)

    """
    # copied from static_builder/cli

    pipeline_metadata, task_iterator = get_pipeline_iterators(spans)
    pipeline_id: str = pipeline_metadata["attributes"]["pipeline.pipeline_run_id"]

    pipeline_artefacts = {
        "dag.mmd": make_mermaid_dag_inputfile(spans, generate_links=True),
        "dag-nolinks.mmd": make_mermaid_dag_inputfile(spans, generate_links=False),
        "gantt.mmd": make_mermaid_gantt_inputfile(spans),
        "pipeline.json": json.dumps(pipeline_metadata, indent=2),
    }

    yield {
        "type": "pipeline",
        "id": pipeline_id,
        "parent_id": None,
        "metadata": pipeline_metadata,
        "artifacts_location": str(Path("pipeline") / pipeline_id),
        "artifacts": [
            {
                "name": k,
                "content": v,
                "size": len(v),
            }
            for k, v in pipeline_artefacts.items()
        ],
    }

    return  #
    for task_artefacts, run_iterator in task_iterator:
        task_metadata = bytes_to_json(task_artefacts["task.json"])
        task_id: str = task_metadata["span_id"]

        yield {
            "type": "task",
            "id": task_id,
            "parent_id": pipeline_id,
            "metadata": task_metadata,
            "artifacts_location": str(
                Path("pipeline") / pipeline_id / "task" / task_id
            ),
            "artifacts": [
                {
                    "name": k,
                    "content": v,
                    "size": len(v),
                }
                for k, v in task_artefacts.items()
            ],
        }

        for run_artefacts in run_iterator:
            run_metadata = bytes_to_json(run_artefacts["run.json"])
            run_id = run_metadata["span_id"]
            yield {
                "type": "run",
                "id": run_id,
                "parent_id": task_id,
                "metadata": run_metadata,
                "artifacts_location": str(
                    Path("pipeline") / pipeline_id / "task" / task_id / "run" / run_id
                ),
                "artifacts": [
                    {
                        "name": k,
                        "content": v,
                        "size": len(v),
                    }
                    for k, v in run_artefacts.items()
                ],
            }


def entry_point():
    print("--- generate_static_data ---")
    print("github_repository          :", args().github_repository)
    print("zip_cache_dir              :", args().zip_cache_dir)
    print("output_www_root_directory  :", args().output_www_root_directory)

    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):
        spans = get_recorded_spans_from_zip(artifact_zip)
        for span_summary in linearize_log_events(spans):
            write_attachment_sink(args().output_www_root_directory, span_summary)

    print("Done")
