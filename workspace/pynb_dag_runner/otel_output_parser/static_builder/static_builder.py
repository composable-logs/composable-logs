import io

from typing import Any, List, Dict, Tuple, Union, Iterable
from pathlib import Path
from zipfile import ZipFile

#
from otel_output_parser.common_helpers.utils import bytes_to_json
from otel_output_parser.common_helpers.graph import Graph


# ---- Implement get_pipeline_artifacts from content of zip file ----

PipelineArtifacts = Dict[str, bytes]
TaskArtifacts = Dict[str, bytes]
RunArtifacts = Dict[str, bytes]


def _directories_under(z, prefix: str) -> List[str]:
    return [
        filepath.filename
        for filepath in z.filelist
        if filepath.filename.startswith(prefix)
        and filepath.is_dir()
        and Path(filepath.filename).parent == Path(prefix)
    ]


def _get_run_artifacts(z: ZipFile, task_run_directory: str) -> Dict[str, bytes]:
    run_dir = Path(task_run_directory)
    run_artifacts = {
        str(Path(filepath.filename).relative_to(run_dir)): z.read(filepath)
        for filepath in z.filelist
        if filepath.filename.startswith(task_run_directory) and not filepath.is_dir()
    }
    assert "run.json" in run_artifacts
    return run_artifacts


def _get_task_artifacts(
    z: ZipFile, task_directory: str
) -> Tuple[TaskArtifacts, List[RunArtifacts]]:
    task_artifacts = {"task.json": z.read(task_directory + "task.json")}

    assert "task.json" in task_artifacts
    return task_artifacts, [
        _get_run_artifacts(z, d) for d in _directories_under(z, task_directory)
    ]


def get_pipeline_artifacts(
    zipfile: Union[Path, bytes],
) -> Tuple[PipelineArtifacts, List[Tuple[TaskArtifacts, List[RunArtifacts]]]]:
    """
    Analogous to function with same name in the main repo. However, this takes as
    input a zip file where json/artifacts have been stored in a directory hierarchy.
    """

    if isinstance(zipfile, bytes):
        z_io: Union[Path, io.BytesIO] = io.BytesIO(zipfile)
    elif isinstance(zipfile, Path):
        z_io = zipfile
    else:
        raise ValueError(f"Unknown zipfile input type {type(zipfile)}")

    with ZipFile(z_io, "r") as z:  # type: ignore

        pipeline_artifacts = {}
        for filepath in [
            "pipeline-outputs/pipeline.json",
            "dag.mmd",
            "dag-nolinks.mmd",
            "dag-diagram.png",
            "gantt.mmd",
            "gantt-diagram.png",
        ]:
            try:
                pipeline_artifacts[Path(filepath).name] = z.read(filepath)
            except:
                pass

        assert "pipeline.json" in pipeline_artifacts

        # We return nested lists (and not iterators). This allows us to close the
        # zipfile before returning.
        return pipeline_artifacts, [
            _get_task_artifacts(z, d) for d in _directories_under(z, "pipeline-outputs")
        ]


# ---- process one zip file; write artifacts/jsons to directory structure ----


def linearize_log_events(zip_content: bytes) -> Iterable[Any]:
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
    pipeline_artefacts, task_iterator = get_pipeline_artifacts(zip_content)
    pipeline_metadata = bytes_to_json(pipeline_artefacts["pipeline.json"])
    pipeline_id: str = pipeline_metadata["attributes"]["pipeline.pipeline_run_id"]

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
