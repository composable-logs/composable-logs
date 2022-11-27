from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
    add_html_notebook_artefacts,
)
from .mermaid_graphs import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
    _status_summary,
)
from .common_helpers.utils import ensure_dir_exist


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
                    _status_summary(task_dict),
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
                    _status_summary(task_run_dict),
                ]
            )

            write_json(
                safe_path(out_basepath / task_dir / run_dir / "run.json"),
                task_run_dict,
            )

            print("     *** run: ", task_run_dict)
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


# --- cli tool implementation ---


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_span_file",
        required=True,
        type=Path,
        help="JSON file with logged OpenTelemetry spans",
    )
    parser.add_argument(
        "--output_directory",
        required=False,
        type=Path,
        help="base output directory for writing tasks and logged artefacts",
    )
    parser.add_argument(
        "--output_filepath_mermaid_gantt",
        required=False,
        type=Path,
        help="output file path for Mermaid Gantt diagram input file (eg. gantt.mmd)",
    )
    parser.add_argument(
        "--output_filepath_mermaid_dag",
        required=False,
        type=Path,
        help="output file path for Mermaid DAG diagram input file (eg. dag.mmd)",
    )
    return parser.parse_args()


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print(f"Number of spans loaded {len(spans)}")

    if args().output_directory is not None:
        write_spans_to_output_directory_structure(spans, args().output_directory)

    if args().output_filepath_mermaid_gantt is not None:
        (
            ensure_dir_exist(args().output_filepath_mermaid_gantt)
            # -
            .write_text(make_mermaid_gantt_inputfile(spans))
        )

    if args().output_filepath_mermaid_dag is not None:
        dag_output_path: Path = ensure_dir_exist(args().output_filepath_mermaid_dag)
        assert dag_output_path.suffix == ".mmd"

        dag_output_path.write_text(
            make_mermaid_dag_inputfile(spans, generate_links=True)
        )

        nolinks_output_path: Path = dag_output_path.with_name(
            dag_output_path.name.replace(".mmd", "-nolinks.mmd")
        )
        nolinks_output_path.write_text(
            make_mermaid_dag_inputfile(spans, generate_links=False)
        )
