import datetime, re
from pathlib import Path
from argparse import ArgumentParser
from typing import List, Tuple

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


def render_seconds(us_range) -> str:
    """
    Convert duration (a range in us) into more human readable format like 1m 20s
    """
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

            if _status_summary(task_run_dict) == "OK":
                modifier = ""
            else:
                modifier = "crit"

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


def make_link_to_task_run(task_run_dict) -> str:
    # -- This is not provided during unit tests
    if "pipeline.github.repository" in task_run_dict["attributes"]:
        repo_owner, repo_name = task_run_dict["attributes"][
            "pipeline.github.repository"
        ].split("/")

        host = f"https://{repo_owner}.github.io/{repo_name}"
    else:
        host = "."

    task_id = (
        task_run_dict["attributes"]["task.notebook"]
        # -
        .split("/")[-1].replace(".py", "")
    )
    run_span_id = task_run_dict["span_id"]

    return f"{host}/#/experiments/{task_id}/runs/{run_span_id}"


def make_mermaid_dag_inputfile(spans: Spans):
    """
    Generate input file for Mermaid diagram generator for creating dependency diagram
    of tasks found in spans.

    Runs are not shown in this diagram.
    """
    output_lines = [
        "graph LR",
        "    %% Mermaid input file for drawing task dependencies ",
        "    %% See https://mermaid-js.github.io/mermaid",
        "    %%",
    ]

    pipeline_dict, task_it = get_pipeline_iterators(spans)

    def dag_node_id(span_id: str) -> str:
        # span_id:s are of form "0x<hex>" and can not be used as Mermaid node_ids as-is.
        return f"TASK_SPAN_ID_{span_id}"

    def dag_node_description(task_dict) -> Tuple[str, List[str]]:
        assert task_dict["attributes"]["task.task_type"] == "jupytext"

        out_lines = []
        for k, v in task_dict["attributes"].items():
            if k.startswith("task.") and k not in ["task.task_type", "task.notebook"]:
                out_lines += [f"{k}={v}"]

        # here one could potentially also add total length of task w.
        # outcome status (success/failure)
        return (
            task_dict["attributes"]["task.notebook"] + " (jupytext task)",
            list(sorted(out_lines)),
        )

    def make_link(desc: str, attrs: List[str], last_run_dict) -> str:
        url: str = make_link_to_task_run(last_run_dict)
        link_html_text: str = f"<b>{desc} 🔗</b> <br />" + "<br />".join(attrs)

        return (
            f"<a href='{url}' style='text-decoration: none; color: black;'>"
            f"{link_html_text}"
            f"</a>"
        )

    for task_dict, run_it in task_it:
        last_run_dict, _ = list(run_it)[-1]

        if task_dict["attributes"]["task.task_type"] != "jupytext":
            raise Exception(f"Unknown task type for {task_dict}")

        linkify = lambda x, ys: make_link(x, ys, last_run_dict)

        output_lines += [
            f"    {dag_node_id(task_dict['span_id'])}"
            f"""["{linkify(*dag_node_description(task_dict))}"]"""
        ]

    for span_id_from, span_id_to in pipeline_dict["task_dependencies"]:
        output_lines += [
            f"    {dag_node_id(span_id_from)} --> {dag_node_id(span_id_to)}"
        ]

    return "\n".join(output_lines)


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
        write_to_output_dir(spans, args().output_directory)

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
