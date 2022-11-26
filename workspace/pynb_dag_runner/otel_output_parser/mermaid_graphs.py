import datetime
from typing import List, Tuple

#
from pynb_dag_runner.opentelemetry_helpers import Spans, get_duration_range_us
from pynb_dag_runner.opentelemetry_task_span_parser import (
    get_pipeline_iterators,
)


def _status_summary(span_dict) -> str:
    if span_dict["status"]["status_code"] == "OK":
        return "OK"
    else:
        return "FAILED"


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


def make_mermaid_dag_inputfile(spans: Spans, generate_links: bool) -> str:
    """
    Generate input file for Mermaid diagram generator for creating dependency diagram
    of tasks found in spans.

    Runs are not shown in this diagram.

    The flag `generate_links` can be used to turn off html links. These do not seem to
    work when converting mermaid into png. (Would an version upgrade fix this?)
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
        if generate_links:
            url: str = make_link_to_task_run(last_run_dict)
            link_html_text: str = f"<b>{desc} 🔗</b> <br />" + "<br />".join(attrs)

            return (
                f"<a href='{url}' style='text-decoration: none; color: black;'>"
                f"{link_html_text}"
                f"</a>"
            )
        else:
            return desc

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
