import glob, json, datetime

from pathlib import Path
from argparse import ArgumentParser

# --- i/o helpers ---


def args():
    parser = ArgumentParser()
    parser.add_argument(
        "--runlogs_root",
        required=True,
        type=str,
        help="root directory for pipeline runlogs",
    )
    parser.add_argument(
        "--output_gantt_mmd",
        type=str,
        required=False,
        help="output filepath for writing Gantt diagram Mermaid file",
    )
    parser.add_argument(
        "--output_dependency_mmd",
        type=str,
        required=False,
        help="output filepath for writing dependency graph Mermaid file",
    )
    args = parser.parse_args()

    return args


def read_json(filepath: str):
    return json.loads(Path(filepath).read_text())


def write_output(filepath: Path, text_content: str):
    print(" - writing output to ", filepath)
    filepath.write_text(text_content)
    print(" - done")


def load_runlogs():
    return [
        read_json(f)
        for f in glob.glob(f"{args().runlogs_root}/**/runlog.json", recursive=True)
    ]


# --- common helper to parse runlog content ---


def runlog_taskname(runlog) -> str:
    if runlog["task_type"] == "PythonNotebookTask":
        return f"""{runlog["notebook_path"]} (nb)"""
    else:
        raise Exception(f"Unknown task_type field in runlog={str(runlog)}")


def make_gantt_mermaid_file_content(runlogs):
    """
    Draw Gantt chart from runtimes
    """

    def runlog_duration_text(runlog) -> str:
        # runtime for a runlog is only used when drawing Gantt-charts
        seconds: float = runlog["out.timing.duration_ms"] / 1000
        if seconds <= 60:
            return f"{round(seconds, 2)}s"
        else:
            dt = datetime.timedelta(seconds=seconds)
            return (
                (str(dt).replace(":", "h ", 1).replace(":", "m ", 1)[:-4] + "s")
                .replace("0h ", "")
                .replace("00m ", "")
            )

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

    for runlog in sorted(runlogs, key=lambda runlog: runlog["out.timing.start_ts"]):
        output_lines += [f"""    section {runlog_taskname(runlog)}"""]

        # render failed tasks in red
        assert runlog["out.status"] in ["SUCCESS", "FAILURE"]
        if runlog["out.status"] == "FAILURE":
            modifier = "crit"
        else:
            modifier = ""

        output_lines += [
            ", ".join(
                [
                    f"""    {runlog_duration_text(runlog)} - {runlog["out.status"]} :{modifier} """,
                    f"""{runlog["out.timing.start_ts"] // 1000000} """,
                    f"""{runlog["out.timing.end_ts"] // 1000000} """,
                ]
            )
        ]

    return "\n".join(output_lines)


def make_dependency_mermaid_file_content(runlogs, task_dependencies):
    """
    Draw task dependency graph
    """
    output_lines = [
        "graph LR",
        "    %% Mermaid input file for drawing task dependencies ",
        "    %% See https://mermaid-js.github.io/mermaid",
        "    %%",
    ]

    # for tasks that have been retried, the same task_id may occur in multiple runlog.json
    # files
    all_task_ids = set([runlog["task_id"] for runlog in runlogs])

    task_id_to_node_name = {
        task_id: f"NODE_{idx}" for idx, task_id in enumerate(all_task_ids)
    }

    def get_unique(xs):
        xs_set = set(xs)
        assert len(xs_set) == 1
        return list(xs_set)[0]

    def get_task_description(task_id):
        # get all runlogs for this task_id
        task_id_runlogs = [runlog for runlog in runlogs if runlog["task_id"] == task_id]
        assert len(task_id_runlogs) >= 1

        result = []

        # add task name as first line in node content
        result += [get_unique(runlog_taskname(runlog) for runlog in task_id_runlogs)]

        # loop over all keys in all runlogs for this task(_id)
        for key in set.union(*[set(runlog.keys()) for runlog in task_id_runlogs]):
            if key.startswith("parameters.task"):
                task_key = key.replace("parameters.task.", "", 1)
                value = get_unique(runlog[key] for runlog in task_id_runlogs)

                # do not render default retry/timout settings
                if task_key == "n_max_retries" and value == 1:
                    continue

                if task_key == "timeout_s" and value is None:
                    continue

                result += [f"{task_key}={value}"]

        return '"' + "<br /> ".join(result) + '"'

    # add one node to the graph per task_id
    for task_id, node_name in task_id_to_node_name.items():
        output_lines += [f"    {node_name}[{get_task_description(task_id)}]"]

    # add arrows between nodes in the graph where tasks are dependent on each other
    for dependency in task_dependencies:
        from_id = dependency["from"]
        to_id = dependency["to"]
        output_lines += [
            f"    {task_id_to_node_name[from_id]} --> {task_id_to_node_name[to_id]}"
        ]

    return "\n".join(output_lines)


# --- main script logic ---

print(" - runlogs_root :", args().runlogs_root)

if args().output_gantt_mmd is not None:
    write_output(
        filepath=Path(args().output_gantt_mmd),
        text_content=make_gantt_mermaid_file_content(load_runlogs()),
    )

if args().output_dependency_mmd is not None:
    write_output(
        filepath=Path(args().output_dependency_mmd),
        text_content=make_dependency_mermaid_file_content(
            runlogs=load_runlogs(),
            task_dependencies=read_json(
                f"{args().runlogs_root}/task_dependencies.json"
            ),
        ),
    )

print(" - render-runlogs script done")

# ---
