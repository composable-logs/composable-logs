from typing import Any, Tuple, List, Iterable
from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId, get_duration_s
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies

# --- span parser ---

FlowDict = Any
TaskDict = Any
RunDict = Any
ArtefactDict = Any


def _artefact_iterator(spans: Spans, task_run_top_span) -> List[ArtefactDict]:
    return []


def _run_iterator(
    spans: Spans, task_top_span
) -> Iterable[Tuple[RunDict, Iterable[ArtefactDict]]]:
    for task_run_top_span in spans.bound_under(task_top_span).filter(
        ["name"], "retry-call"
    ):
        run_dict = {
            "run_span_id": task_run_top_span["context"]["span_id"],
            "start_time": task_run_top_span["start_time"],
            "end_time": task_run_top_span["end_time"],
            "duration_s": get_duration_s(task_run_top_span),
            "status": task_run_top_span["status"],
            "run_attributes": (
                spans.bound_inclusive(task_run_top_span)
                #
                .get_attributes(allowed_prefixes={"run."})
            ),
        }
        yield run_dict, _artefact_iterator(spans, task_run_top_span)

    return iter([])


def _task_iterator(spans: Spans) -> Iterable[Tuple[TaskDict, Iterable[RunDict]]]:
    for task_top_span in spans.filter(["name"], "execute-task"):
        task_dict = {
            "task_span_id": task_top_span["context"]["span_id"],
            "start_time": task_top_span["start_time"],
            "end_time": task_top_span["end_time"],
            "duration_s": get_duration_s(task_top_span),
            "status": task_top_span["status"],
            "task_dict": (
                spans.bound_inclusive(task_top_span)
                #
                .get_attributes(allowed_prefixes={"task."})
            ),
        }

        yield task_dict, _run_iterator(spans, task_top_span)

    return iter([])


def get_flow_iterators(
    spans: Spans,
) -> Tuple[FlowDict, Iterable[Tuple[TaskDict, Iterable[RunDict]]]]:
    flow_attributes = {
        "task_dependencies": list(extract_task_dependencies(spans)),
        "flow_attributes": spans.get_attributes(allowed_prefixes={"flow."}),
    }

    return flow_attributes, _task_iterator(spans)


def _status_summary(span_dict) -> str:
    if span_dict["status"]["status_code"] == "OK":
        return "OK"
    else:
        return "FAILED"


def write_to_output_dir(spans: Spans, output_basepath: Path):
    print(" - Writing tasks in spans to output_basepath", output_basepath)
    output_basepath.mkdir(parents=True, exist_ok=True)

    flow_dict, task_it = get_flow_iterators(spans)
    write_json(output_basepath / "flow.json", flow_dict)

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
            print("-- retry --")
            print(task_run_dict)

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
        "--output_basepath",
        required=False,
        type=Path,
        help="output directory for writing tasks and logged artefacts",
    )
    return parser.parse_args()


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print("nr of spans loaded", len(spans))

    if args().output_basepath is not None:
        write_to_output_dir(spans, args().output_basepath)
