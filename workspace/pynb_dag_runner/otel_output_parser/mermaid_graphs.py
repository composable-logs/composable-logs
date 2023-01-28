import datetime
from typing import List, Tuple

#
from pynb_dag_runner.opentelemetry_helpers import Spans
from pynb_dag_runner.opentelemetry_task_span_parser import parse_spans


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


def make_link_to_task_run(task_summary) -> str:
    # -- This is not provided during unit tests
    if "workflow.github.repository" in task_summary.attributes:
        repo_owner, repo_name = task_summary.attributes[
            "workflow.github.repository"
        ].split("/")

        host = f"https://{repo_owner}.github.io/{repo_name}"
    else:
        host = "."

    # Determine experiment name, eg 'notebooks/summary.py' -> 'summary'
    task_id = task_summary.attributes["task.notebook"].split("/")[-1].replace(".py", "")

    # Breaking change 12/2022:
    #    previously last span was id for run of task. This is now changed to the
    #    span-id of the task (and we hereafter assume that there is only one run
    #    per task).
    return f"{host}/#/experiments/{task_id}/runs/{task_summary.span_id}"


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

    pipeline_summary = parse_spans(spans)

    def dag_node_id(span_id: str) -> str:
        # span_id:s are of form "0x<hex>" and can not be used as Mermaid node_ids as-is.
        return f"TASK_SPAN_ID_{span_id}"

    def dag_node_description(attributes) -> Tuple[str, List[str]]:
        assert attributes["task.task_type"] == "jupytext"

        out_lines = []
        for k, v in attributes.items():
            if k.startswith("task.") and k not in ["task.task_type", "task.notebook"]:
                out_lines += [f"{k}={v}"]

        # here one could potentially also add total length of task w.
        # outcome status (success/failure)
        return (
            attributes["task.notebook"] + " (jupytext task)",
            list(sorted(out_lines)),
        )

    def make_link(desc: str, attrs: List[str], task_summary) -> str:
        if generate_links:
            url: str = make_link_to_task_run(task_summary)
            link_html_text: str = f"<b>{desc} 🔗</b> <br />" + "<br />".join(attrs)

            return (
                f"<a href='{url}' style='text-decoration: none; color: black;'>"
                f"{link_html_text}"
                f"</a>"
            )
        else:
            return desc

    for task_summary in pipeline_summary.task_runs:
        if task_summary.attributes["task.task_type"] != "jupytext":
            raise Exception(f"Unknown task type for {task_summary}")

        linkify = lambda x, ys: make_link(x, ys, task_summary)

        output_lines += [
            f"    {dag_node_id(task_summary.span_id)}"
            f"""["{linkify(*dag_node_description(task_summary.attributes))}"]"""
        ]

    # add links between noded in graph
    for span_id_from, span_id_to in pipeline_summary.task_dependencies:
        output_lines += [
            f"    {dag_node_id(span_id_from)} --> {dag_node_id(span_id_to)}"
        ]

    return "\n".join(output_lines)


def make_mermaid_gantt_inputfile(spans: Spans) -> str:
    """
    Generate input file for Mermaid diagram generator for creating Gantt diagram
    of tasks that have run.
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
    pipeline_summary = parse_spans(spans)

    for task_run_summary in pipeline_summary.task_runs:
        attributes = task_run_summary.attributes

        if attributes["task.task_type"] != "jupytext":
            raise Exception(f"Unknown task type for {task_run_summary.attributes}")

        output_lines += [f"""    section {attributes["task.notebook"]}"""]

        if task_run_summary.is_success():
            description = "OK"
            modifier = ""
        else:
            description = "FAILED"
            modifier = "crit"

        us_range = task_run_summary.timing.get_task_timestamp_range_us_epoch()
        output_lines += [
            ", ".join(
                [
                    f"""    {render_seconds(us_range)} - {description} :{modifier} """,
                    f"""{us_range.start // 1000000} """,
                    f"""{us_range.stop // 1000000} """,
                ]
            )
        ]

    return "\n".join(output_lines)
