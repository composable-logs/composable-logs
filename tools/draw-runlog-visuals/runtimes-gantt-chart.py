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
        "--gantt_mmd_outputfile",
        type=str,
        required=False,
        help="Output filepath for writing Mermaid input file",
    )
    args = parser.parse_args()

    return args


def load_runlogs():
    return [
        json.loads(Path(f).read_text())
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

if args().gantt_mmd_outputfile is not None:
    print(" - writing Mermaid Gantt output file to :", args().gantt_mmd_outputfile)
    Path(args().gantt_mmd_outputfile).write_text(
        make_gantt_mermaid_file_content(load_runlogs())
    )
    print(f"    done!")

print("Done")
