from pathlib import Path
from argparse import ArgumentParser

# -
from pynb_dag_runner import version_string
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans

# -
from .mermaid_graphs import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
)
from .common_helpers.utils import ensure_dir_exist


def _status_summary(span_dict) -> str:
    if span_dict["status"]["status_code"] == "OK":
        return "OK"
    else:
        return "FAILED"


def safe_path(filepath: Path):
    assert str(filepath).startswith("/")
    assert ".." not in str(filepath)
    return filepath


def outcome(is_success: bool) -> str:
    if is_success:
        return "OK"
    else:
        return "FAILED"


def write_spans_to_output_directory_structure(spans: Spans, out_basepath: Path):
    """
    Write out tasks/runs/artefacts found in spans into a directory structure for
    inspection using a file browser.

    Any notebooks logged are written to the directory structure both in
    ipynb and html formats.
    """
    print(" - Writing tasks in spans to ", out_basepath)

    workflow_summary = parse_spans(spans)

    write_json(
        safe_path(out_basepath / "run-time-metadata.json"), workflow_summary.as_dict()
    )

    for task_run_summary in workflow_summary.task_runs:
        # -- write json with task-specific data --
        if task_run_summary.attributes["task.task_type"] == "jupytext":
            task_dir: str = "--".join(
                [
                    "jupytext-notebook-task",
                    task_run_summary.attributes["task.notebook"]  # type: ignore
                    .replace("/", "-")  # type: ignore
                    .replace(".", "-"),  # type: ignore
                    task_run_summary.span_id,
                    outcome(task_run_summary.is_success()),
                ]
            )

        else:
            raise Exception(f"Unknown task type for {task_run_summary.attributes}")

        write_json(
            safe_path(out_basepath / task_dir / "run-time-metadata.json"),
            task_run_summary.as_dict(),
        )

        for artifact in task_run_summary.logged_artifacts:
            out_path: Path = safe_path(
                ensure_dir_exist(out_basepath / task_dir / "artifacts" / artifact.name)
            )
            artifact.write(out_path)


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
    print(f"--- pynb_log_parser cli {version_string()} ---")

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

    print(" - Done")
