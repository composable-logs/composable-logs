import io, json

from typing import Any, List, Dict, Tuple, Union
from pathlib import Path
from zipfile import ZipFile


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

        pipeline_artifacts = {
            Path(filepath).name: z.read(filepath)
            for filepath in [
                "pipeline-outputs/pipeline.json",
                "dag-diagram.png",
                "gantt-diagram.png",
            ]
        }
        assert "pipeline.json" in pipeline_artifacts

        # We return nested lists (and not iterators). This allows us to close the
        # zipfile before returning.
        return pipeline_artifacts, [
            _get_task_artifacts(z, d) for d in _directories_under(z, "pipeline-outputs")
        ]


# ---- process one zip file; write artifacts/jsons to directory structure ----


def bytes_to_json(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


def ensure_dir_exist(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def process_artifact(zip_content: bytes, output_dir: Path):
    pipeline_artefacts, task_iterator = get_pipeline_artifacts(zip_content)
    pipeline_id: str = (
        bytes_to_json(pipeline_artefacts["pipeline.json"])
        # -
        ["attributes"]["pipeline.pipeline_run_id"]
    )

    print()
    print(f"pipeline-id {pipeline_id}")

    for k, v in pipeline_artefacts.items():
        print(f" - writing pipeline-artefact {k} ({len(v)} bytes)")
        ensure_dir_exist(output_dir / "pipeline" / pipeline_id / k).write_bytes(v)

    for task_artefacts, run_iterator in task_iterator:
        task_id: str = bytes_to_json(task_artefacts["task.json"])["span_id"]
        print(f"    - task_id {task_id}*")

        for k, v in task_artefacts.items():
            print(f"      writing task-artefact {k} ({len(v)} bytes)")
            ensure_dir_exist(
                output_dir / "pipeline" / pipeline_id / "task" / task_id / k
            ).write_bytes(v)

        for run_artefacts in run_iterator:
            run_id: str = bytes_to_json(run_artefacts["run.json"])["span_id"]
            print(f"       - run_id {run_id}")

            for k, v in run_artefacts.items():
                print(f"         writing run-artefact {k} ({len(v)}) bytes)")
                ensure_dir_exist(
                    output_dir
                    / "pipeline"
                    / pipeline_id
                    / "task"
                    / task_id
                    / "run"
                    / run_id
                    / k
                ).write_bytes(v)

    print("Done. Processed all pipeline, task and run:s")
