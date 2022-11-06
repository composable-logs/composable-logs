import re
from pathlib import Path
from argparse import ArgumentParser

#
from pynb_dag_runner.helpers import read_json
from pynb_dag_runner.opentelemetry_helpers import (
    Spans,
)

#
from .otel_log_processors.directory_output_writer import (
    write_spans_to_output_directory_structure,
)
from .otel_log_processors.mermaid_extractors import (
    make_mermaid_dag_inputfile,
    make_mermaid_gantt_inputfile,
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
        args().output_filepath_mermaid_gantt.write_text(
            make_mermaid_gantt_inputfile(spans)
        )

    if args().output_filepath_mermaid_dag is not None:
        dag_output_path: Path = args().output_filepath_mermaid_dag
        assert dag_output_path.suffix == ".mmd"

        dag_mmd_content: str = make_mermaid_dag_inputfile(spans)
        dag_output_path.write_text(dag_mmd_content)

        # v--- temp hack
        # - Converting mmd with html links to png does not work so remove any link
        nolinks_dag_mmd_content: str = (
            re.sub(r"<a href=.*?>", "", dag_mmd_content)
            .replace("</a>", "")
            .replace("🔗", "")
            .replace("<b>", "")
            .replace("</b>", "")
        )

        nolinks_output_path: Path = dag_output_path.with_name(
            dag_output_path.name.replace(".mmd", "-nolinks.mmd")
        )
        nolinks_output_path.write_text(nolinks_dag_mmd_content)
        # ^---
