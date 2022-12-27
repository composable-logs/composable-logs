import io, json
from zipfile import ZipFile
from typing import Any, Iterable

from pathlib import Path
from functools import lru_cache
from argparse import ArgumentParser

# -
from pynb_dag_runner.helpers import disjoint_dict_union
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
    add_html_notebook_artefacts,
    parse_spans,
    ArtifactName,
    ArtifactContent,
)

# -
from .mermaid_graphs import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
)
from .static_builder.writers import write_attachment_sink_old, StaticMLFlowDataSinkOld
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

    for task_dict, task_run_it in task_iterator:
        task_id: str = task_dict["span_id"]
        task_json: str = json.dumps(task_dict, indent=2)

        yield {
            "type": "task",
            "id": task_id,
            "parent_id": pipeline_id,
            "metadata": task_dict,
            "artifacts_location": str(
                Path("pipeline") / pipeline_id / "task" / task_id
            ),
            "artifacts": [
                {
                    "name": "task.json",
                    "content": task_json,
                    "size": len(task_json),
                }
            ],
        }

        for task_run_dict, task_run_artefacts in task_run_it:
            run_id = task_run_dict["span_id"]

            run_artifacts = {
                "run.json": json.dumps(task_run_dict, indent=2),
            }

            for run_artefacts_dict in add_html_notebook_artefacts(task_run_artefacts):
                assert run_artefacts_dict["name"] != "run.json"
                run_artifacts[run_artefacts_dict["name"]] = run_artefacts_dict[
                    "content"
                ]

            yield {
                "type": "run",
                "id": run_id,
                "parent_id": task_id,
                "metadata": task_run_dict,
                "artifacts_location": str(
                    Path("pipeline") / pipeline_id / "task" / task_id / "run" / run_id
                ),
                "artifacts": [
                    {
                        "name": k,
                        "content": v,
                        "size": len(v),
                    }
                    for k, v in run_artifacts.items()
                ],
            }


def _write_artifacts(output_path: Path, artifacts):
    for artifact_name, artifact_content in artifacts.items():
        artifact_content.write(ensure_dir_exist(output_path / artifact_name))
        yield artifact_name, artifact_content


def _artifact_metadata(artifacts_items):
    for artifact_name, artifact_content in artifacts_items:
        yield {
            "name": artifact_name,
            "type": artifact_content.type,
            "size": len(artifact_content.content),
        }


def process(spans: Spans, www_root: Path):
    pipeline_summary = parse_spans(spans)
    print("> processing pipeline run", pipeline_summary.span_id)

    pipeline_artifact_relative_root = (
        Path("artifacts") / "pipeline" / pipeline_summary.span_id
    )

    # The below are not real artifacts logged during pipeline execution.
    #
    # Rather, these are assets generated for reporting/visualisation.
    #
    # We want to generate these *after the pipeline has run* so these
    # can be generated dynamically and updated independently without having
    # to rerun the pipeline.
    #
    reporting_artifacts = {
        "dag.mmd": ArtifactContent(
            type="utf-8",
            content=make_mermaid_dag_inputfile(spans, generate_links=True),
        ),
        "dag-nolinks.mmd": ArtifactContent(
            type="utf-8",
            content=make_mermaid_dag_inputfile(spans, generate_links=False),
        ),
        "gantt.mmd": ArtifactContent(
            type="utf-8",
            content=make_mermaid_gantt_inputfile(spans),
        ),
        "run-time-metadata.json": ArtifactContent(
            type="utf-8",
            content=json.dumps(pipeline_summary.as_dict(), indent=2),
        ),
    }

    # Note: This dict is optimised for UI/reporting. It is slightly different from
    # the summary dict created at pipeline runtime.
    yield {
        "span_id": pipeline_summary.span_id,
        "type": "pipeline",
        "artifacts_location": str(pipeline_artifact_relative_root),
        "start_time_epoch_us": pipeline_summary.get_start_time_epoch_us(),
        "end_time_epoch_us": pipeline_summary.get_end_time_epoch_us(),
        "duration_s": pipeline_summary.get_duration_s(),
        "is_success": pipeline_summary.is_success(),
        "parent_id": None,
        "attributes": pipeline_summary.attributes,
        "artifacts": list(
            _artifact_metadata(
                _write_artifacts(
                    output_path=www_root / pipeline_artifact_relative_root,
                    artifacts=reporting_artifacts,
                )
            )
        ),
    }

    for task_run_summary in pipeline_summary.task_runs:
        task_artifact_root = www_root / "artifacts" / "task" / task_run_summary.span_id
        print(" - task", task_artifact_root)
        yield {
            "span_id": task_run_summary.span_id,
            "type": "task",
            "artifacts_location": str(task_artifact_root),
            "start_time_epoch_us": task_run_summary.get_start_time_epoch_us(),
            "end_time_epoch_us": task_run_summary.get_end_time_epoch_us(),
            "duration_s": task_run_summary.get_duration_s(),
            "is_success": task_run_summary.is_success(),
            "parent_id": pipeline_summary.span_id,
            "attributes": task_run_summary.attributes,
            "artifacts": list(
                _artifact_metadata(
                    _write_artifacts(
                        output_path=www_root / task_artifact_root,
                        # log artifacts logged during task run.
                        # in addition, log a json with metadata logged *during run time*
                        artifacts=disjoint_dict_union(
                            task_run_summary.logged_artifacts,
                            {
                                "run-time-metadata.json": ArtifactContent(
                                    type="utf-8",
                                    content=json.dumps(
                                        task_run_summary.as_dict(), indent=2
                                    ),
                                )
                            },
                        ),
                    )
                )
            ),
        }


def entry_point():
    print("--- generate_static_data ---")
    print("github_repository          :", args().github_repository)
    print("zip_cache_dir              :", args().zip_cache_dir)
    print("output_www_root_directory  :", args().output_www_root_directory)

    # --- old parser : to be deleted after move to new span parser ---

    # if output_static_data_json is None, this sink is no-op
    static_mlflow_data_sink = StaticMLFlowDataSinkOld(
        args().output_www_root_directory / "ui_static_data.json"
    )

    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):
        spans = get_recorded_spans_from_zip(artifact_zip)
        span_summary = list(linearize_log_events(spans))

        print(
            "pipeline ids:",
            [s["id"] for s in span_summary if s["type"] == "pipeline"],
        )

        for span_summary in linearize_log_events(spans):
            write_attachment_sink_old(
                args().output_www_root_directory / "pipeline-artifacts",
                span_summary,
            )
            static_mlflow_data_sink.push(span_summary)

    static_mlflow_data_sink.close()
    # ---- end of old parser --- to be deleted ---

    # --- new span parser ---

    entries = []
    for artifact_zip in github_repo_artifact_zips(
        github_repository=args().github_repository,
        zip_cache_dir=args().zip_cache_dir,
    ):
        print(f"--- Processing new zip with {len(spans)} spans ...")
        spans = get_recorded_spans_from_zip(artifact_zip)

        for entry in process(
            spans, args().output_www_root_directory / "static-artifacts"
        ):

            entries.append(entry)

    (
        ensure_dir_exist(args().output_www_root_directory / "static_data.json")
        # -
        .write_text(json.dumps(entries, indent=2))
    )

    print("Done")
