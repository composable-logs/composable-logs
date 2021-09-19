import glob, json, datetime

from pathlib import Path
from argparse import ArgumentParser


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


def load_runlogs():
    return [
        read_json(f)
        for f in glob.glob(f"{args().runlogs_root}/**/runlog.json", recursive=True)
    ]


def runlog_taskname(runlog) -> str:
    if runlog["task_type"]:
        return runlog["notebook_path"]
    else:
        raise Exception(f"Unknown task_type field in runlog={str(runlog)}")


def make_gantt_mermaid_file_content(runlogs):
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


print(" - runlogs_root :", args().runlogs_root)

if args().output_gantt_mmd is not None:
    print(" - writing Mermaid Gantt output file to :", args().output_gantt_mmd)
    Path(args().output_gantt_mmd).write_text(
        make_gantt_mermaid_file_content(load_runlogs())
    )
    print(f"    done!")


def make_dependency_mermaid_file_content(runlogs):
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

    def get_task_description(task_id):
        return task_id

    # add one node to the graph per task_id
    for task_id, node_name in task_id_to_node_name.items():
        output_lines += [f"    {node_name}[{get_task_description(task_id)}]"]

    # add arrows between nodes in the graph where tasks are dependent on each other
    for dependency in read_json(f"{args().runlogs_root}/task_dependencies.json"):
        from_id = dependency["from"]
        to_id = dependency["to"]
        output_lines += [
            f"    {task_id_to_node_name[from_id]} --> {task_id_to_node_name[to_id]}"
        ]

    return "\n".join(output_lines)


if args().output_dependency_mmd is not None:
    print(
        " - writing Mermaid dependency graph output file to :",
        args().output_dependency_mmd,
    )
    Path(args().output_dependency_mmd).write_text(
        make_dependency_mermaid_file_content(load_runlogs())
    )
    print(f"    done!")


print("Done")
