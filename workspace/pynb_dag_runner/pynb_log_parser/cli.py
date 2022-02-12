import datetime
from pathlib import Path
from argparse import ArgumentParser


#
from pynb_dag_runner.helpers import read_json, write_json
from pynb_dag_runner.opentelemetry_helpers import Spans, get_duration_range_us
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
    add_html_notebook_artefacts,
)


def _status_summary(span_dict) -> str:
    if span_dict["status"]["status_code"] == "OK":
        return "OK"
    else:
        return "FAILED"


def write_to_output_dir(spans: Spans, out_basepath: Path):
    """
    Write out tasks/runs/artefacts found in spans into a directory structure for
    inspection using a file browser.

    Any notebooks logged are written to the directory structure both in
    ipynb and html formats.
    """
    print(" - Writing tasks in spans to ", out_basepath)

    pipeline_dict, task_it = get_pipeline_iterators(spans)

    def safe_path(path: Path):
        assert str(path).startswith("/")
        assert ".." not in str(path)
        return path

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
                artefact_encoding: str = artefact_dict["encoding"]
                artefact_content: str = artefact_dict["content"]

                print(f"         *** artefact: {artefact_name} ({artefact_encoding})")

                if artefact_encoding == "text/utf-8":
                    out_path: Path = out_basepath / task_dir / run_dir / artefact_name
                    safe_path(out_path).write_text(artefact_content)
                else:
                    raise ValueError(
                        f"Unknown encoding of artefect: {str(artefact_dict)[:2000]}"
                    )


def make_mermaid_gantt_inputfile(spans: Spans) -> str:
    """
    Generate input file for Mermaid diagram generator for creating Gantt diagram
    of tasks/runs found in spans.

    """
    output_lines = [
        "gantt",
        "    %% Mermaid input file for drawing Gantt chart of runlog runtimes",
        "    %% See https://mermaid-js.github.io/mermaid/#/gantt",
        "    %%",
        "    axisFormat %H:%M",
        "    %%",
        "    %% Give timestamps as unix timestamps (ms)",
        "    dateFormat x",
        "    %%",
    ]

    def render_seconds(us_range) -> str:
        "Convert duration is seconds into more human readable format"
        seconds: float = (us_range.stop - us_range.start) / 1e6
        if seconds <= 60:
            return f"{round(seconds, 2)}s"
        else:
            dt = datetime.timedelta(seconds=seconds)
            return (
                (str(dt).replace(":", "h ", 1).replace(":", "m ", 1)[:-4] + "s")
                .replace("0h ", "")
                .replace("00m ", "")
            )

    _, task_it = get_pipeline_iterators(spans)

    for task_dict, task_retry_it in task_it:
        print("task", task_dict)
        print(get_duration_range_us(task_dict).start)

        # -- write json with task-specific data --
        if task_dict["attributes"]["task.task_type"] != "jupytext":
            raise Exception(f"Unknown task type for {task_dict}")

        output_lines += [f"""    section {task_dict["attributes"]["task.notebook"]}"""]

        for task_run_dict, _ in task_retry_it:
            print("run", task_run_dict)
            modifier = ""
            # if runlog["out.status"] == "FAILURE":
            #     modifier = "crit"
            us_range = get_duration_range_us(task_run_dict)

            output_lines += [
                ", ".join(
                    [
                        f"""    {render_seconds(us_range)} - {_status_summary(task_run_dict)} :{modifier} """,
                        f"""{us_range.start // 1000000} """,
                        f"""{us_range.stop // 1000000} """,
                    ]
                )
            ]

    return "\n".join(output_lines)


# --- cli tool implementation ---

# Example usage:
#
# pynb_log_parser --input_span_file pynb_log_parser/opentelemetry-spans.json --output_directory pynb_log_parser/tmp
# pynb_log_parser --input_span_file pynb_log_parser/opentelemetry-spans.json --output_filepath_mermaid_gantt pynb_log_parser/gantt.mmd


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
    return parser.parse_args()


def entry_point():
    print("-- pynb_dag_runner: log parser cli --")

    spans: Spans = Spans(read_json(args().input_span_file))
    print(f"Number of spans loaded {len(spans)}")

    if args().output_directory is not None:
        write_to_output_dir(spans, args().output_directory)

    if args().output_filepath_mermaid_gantt is not None:
        args().output_filepath_mermaid_gantt.write_text(
            make_mermaid_gantt_inputfile(spans)
        )
