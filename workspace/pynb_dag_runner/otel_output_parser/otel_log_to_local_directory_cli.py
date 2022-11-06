from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json
from pynb_dag_runner.opentelemetry_helpers import Spans

#
from .otel_log_processors.directory_output_writer import (
    write_spans_to_output_directory_structure,
)
from .otel_log_processors.mermaid_extractors import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
)


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--input_span_file",
        required=True,
        type=Path,
        help="JSON file with OpenTelemetry spans from a pynb-dag-runner pipeline run",
    )
    parser.add_argument(
        "--output_directory",
        required=True,
        type=Path,
        help="base output directory for expanding tasks, artifacts and mermaid diagrams",
    )

    return parser.parse_args()


def entry_point():
    print(
        "--- otel_log_to_local_directory: "
        "Expand one OpenTelemetry log into a directory structure "
        "---"
    )

    spans: Spans = Spans(read_json(args().input_span_file))
    print(f" *** Number of spans loaded {len(spans)}")

    write_spans_to_output_directory_structure(
        spans, args().output_directory / "pipeline-outputs"
    )

    (args().output_directory / "gantt.mmd").write_text(
        make_mermaid_gantt_inputfile(spans)
    )

    (args().output_directory / "dag.mmd").write_text(
        make_mermaid_dag_inputfile(spans=spans, generate_links=True)
    )

    (args().output_directory / "dag-nolinks.mmd").write_text(
        make_mermaid_dag_inputfile(spans=spans, generate_links=False)
    )

    print(" *** Done")
