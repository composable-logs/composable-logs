from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import get_pipeline_iterators


def _status_summary(span_dict) -> str:
    if span_dict["status"]["status_code"] == "OK":
        return "OK"
    else:
        return "FAILED"


def write_to_output_dir(spans: Spans, output_basepath: Path):
    print(" - Writing tasks in spans to output_basepath", output_basepath)

    pipeline_dict, task_it = get_pipeline_iterators(spans)

    output_basepath.mkdir(parents=True, exist_ok=True)
    write_json(output_basepath / "pipeline.json", pipeline_dict)

    for task_dict, task_retry_it in task_it:
        if task_dict["task_dict"]["task.task_type"] == "jupytext":
            task_subdir: str = "--".join(
                [
                    "jupytext-notebook-task",
                    task_dict["task_dict"]["task.notebook"]
                    .replace("/", "-")
                    .replace(".", "-"),
                    task_dict["task_span_id"],
                    _status_summary(task_dict),
                ]
            )

        else:
            raise Exception(f"Unknown task type for {task_dict}")

        task_basepath: Path = output_basepath / task_subdir

        task_basepath.mkdir(parents=True, exist_ok=True)
        write_json(task_basepath / "task.json", task_dict)

        for task_run_dict, task_run_artefacts in task_retry_it:
            run_basepath: Path = task_basepath / "--".join(
                [
                    f"run={task_run_dict['run_attributes']['run.retry_nr']}",
                    task_run_dict["run_span_id"],
                    _status_summary(task_run_dict),
                ]
            )

            run_basepath.mkdir(parents=True, exist_ok=True)
            write_json(run_basepath / "run.json", task_run_dict)

            for artefact in task_run_artefacts:
                pass


# --- cli tool implementation ---

# Example usage:
#
# pynb_log_parser --input_span_file pynb_log_parser/opentelemetry-spans.json --output_basepath pynb_log_parser/tmp


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
    return parser.parse_args()


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print("nr of spans loaded", len(spans))

    if args().output_directory is not None:
        write_to_output_dir(spans, args().output_directory)
