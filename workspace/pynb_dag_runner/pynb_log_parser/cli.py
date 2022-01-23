from typing import Any, Tuple
from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId, get_duration_s
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies

# --- span parser ---

FlowDict = Any
TaskIterator = Any
TaskDict = Any
RunIterator = Any


def _artefact_iterator(spans: Spans, run_top_span):
    return []


def _run_iterator(spans: Spans, task_top_span):
    return []


def _task_iterator(spans: Spans) -> Tuple[TaskDict, RunIterator]:
    for task_top_span in spans.filter(["name"], "execute-task"):
        task_attributes = {
            "task_span_id": task_top_span["context"]["span_id"],
            "start_time": task_top_span["start_time"],
            "end_time": task_top_span["end_time"],
            "duration_s": get_duration_s(task_top_span),
            "status": task_top_span["status"],
            "task_attributes": (
                spans.bound_inclusive(task_top_span)
                #
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        yield task_attributes, _run_iterator(spans, task_top_span)


def get_flow_iterators(spans: Spans) -> Tuple[FlowDict, TaskIterator]:
    flow_attributes = {
        "task_dependencies": list(extract_task_dependencies(spans)),
        "flow_attributes": spans.get_attributes(allowed_prefixes={"flow."}),
    }

    return flow_attributes, _task_iterator(spans)


def write_to_output_dir(spans: Spans, output_directory: Path):
    print(" - Writing tasks in spans to output_directory", output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    flow_dict, task_it_it_it = get_flow_iterators(spans)
    write_json(output_directory / "flow.json", flow_dict)

    for task_dict, retry_it in task_it_it_it:
        if task_dict["task_attributes"]["task.task_type"] == "jupytext":
            success_indicator = (
                "OK" if task_dict["status"]["status_code"] == "OK" else "FAILED"
            )
            dirname = [
                "jupytext-notebook-task",
                task_dict["task_attributes"]["task.notebook"]
                .replace("/", "-")
                .replace(".", "-"),
                task_dict["task_span_id"],
                success_indicator,
            ]
            task_dir_name = output_directory / "--".join(dirname)

        else:
            raise Exception("Unknown task type")

        task_filepath: Path = output_directory / task_dir_name
        task_filepath.mkdir(parents=True, exist_ok=False)
        write_json(task_filepath / "task.json", task_dict)

        for retry_attributes, artefacts_it in retry_it:
            print("-- retry --")
            print(retry_attributes)

            for artefact in artefacts_it:
                pass


# --- cli tool implementation ---

# Example usage:
#
# pynb_log_parser --input_span_file pynb_log_parser/opentelemetry-spans.json --output_directory pynb_log_parser/tmp


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
        help="output directory for writing tasks and logged artefacts",
    )
    return parser.parse_args()


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print("nr of spans loaded", len(spans))

    if args().output_directory is not None:
        write_to_output_dir(spans, args().output_directory)
