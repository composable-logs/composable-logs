from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json

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

    spans = read_json(args().input_span_file)
    print("nr of spans", len(spans))
