from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans, SpanId
from pynb_dag_runner.opentelemetry_task_span_parser import extract_task_dependencies


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


def write_to_output_dir(spans: Spans, output_directory: Path):
    print(" - Writing tasks in spans to output_directory", output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    # write task dependencies
    write_json(
        output_directory / "task_dependencies.json",
        list(extract_task_dependencies(spans)),
    )


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print("nr of spans loaded", len(spans))

    if args().output_directory is not None:
        write_to_output_dir(spans, args().output_directory)
