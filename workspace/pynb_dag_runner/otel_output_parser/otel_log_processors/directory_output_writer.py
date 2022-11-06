from pathlib import Path

#
from pynb_dag_runner.helpers import write_json
from pynb_dag_runner.opentelemetry_helpers import Spans, get_span_status
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
    add_html_notebook_artefacts,
)


def write_spans_to_output_directory_structure(spans: Spans, out_basepath: Path):
    """
    Write out tasks/runs/artefacts found in spans into a directory structure for
    inspection using a file browser.

    Any notebooks logged are written to the directory structure both in
    ipynb and html formats.
    """
    print(" - Writing tasks in spans to ", out_basepath)

    pipeline_dict, task_it = get_pipeline_iterators(spans)

    def safe_path(filepath: Path):
        assert str(filepath).startswith("/")
        assert ".." not in str(filepath)
        return filepath

    # -- write json with pipeline-specific data --
    write_json(safe_path(out_basepath / "pipeline.json"), pipeline_dict)

    for task_dict, task_retry_it in task_it:
        # -- write json with task-specific data --
        if task_dict["attributes"]["task.task_type"] == "jupytext":
            task_dir: str = "--".join(
                [
                    "jupytext-notebook-task",
                    task_dict["attributes"]["task.notebook"]
                    .replace("/", "-")
                    .replace(".", "-"),
                    task_dict["span_id"],
                    get_span_status(task_dict),
                ]
            )

        else:
            raise Exception(f"Unknown task type for {task_dict}")

        write_json(safe_path(out_basepath / task_dir / "task.json"), task_dict)

        print("*** task: ", task_dict)

        for task_run_dict, task_run_artefacts in task_retry_it:
            # -- write json with run-specific data --
            run_dir: str = "--".join(
                [
                    f"run={task_run_dict['attributes']['run.retry_nr']}",
                    task_run_dict["span_id"],
                    get_span_status(task_run_dict),
                ]
            )

            write_json(
                safe_path(out_basepath / task_dir / run_dir / "run.json"),
                task_run_dict,
            )

            print("     *** run: ", task_run_dict)

            # TODO: move ipynb -> html conversion here
            for artefact_dict in add_html_notebook_artefacts(task_run_artefacts):
                # -- write artefact logged to run --
                artefact_name: str = artefact_dict["name"]
                artefact_type: str = artefact_dict["type"]
                artefact_content: str = artefact_dict["content"]

                print(f"         *** artefact: {artefact_name} ({artefact_type})")

                out_path: Path = out_basepath / task_dir / run_dir / artefact_name
                if artefact_type == "utf-8":
                    safe_path(out_path).write_text(artefact_content)
                elif artefact_type == "bytes":
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    safe_path(out_path).write_bytes(artefact_content)
                else:
                    raise ValueError(
                        f"Unknown encoding of artefect: {str(artefact_dict)[:2000]}"
                    )
