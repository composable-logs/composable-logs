import glob, json, datetime

from pathlib import Path
from argparse import ArgumentParser

#

parser = ArgumentParser()
parser.add_argument(
    "--runlogs_root", type=str, help="root directory for pipeline runlogs",
)
parser.add_argument(
    "--output", type=str, help="Output filepath for writing Mermaid input file"
)
args = parser.parse_args()

print("*** Command line parameters ***")
print("  - runlogs_root   :", args.runlogs_root)
print("  - output         :", args.output)


def load_sorted_runlogs():
    runlogs = [
        json.loads(Path(f).read_text())
        for f in glob.glob(f"{args.runlogs_root}/**/runlog.json", recursive=True)
    ]
    return sorted(runlogs, key=lambda runlog: runlog["out.timing.start_ts"])


def runlog_taskname(runlog) -> str:
    if runlog["task_type"]:
        return runlog["notebook_path"]
    else:
        raise Exception(f"Unknown task_type field in runlog={str(runlog)}")


def runlog_duration_text(runlog) -> str:
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


with open(args.output, "wt") as f:
    f.write(
        """gantt
    %% Mermaid input file for drawing Gantt chart of runlog runtimes
    %% See https://mermaid-js.github.io/mermaid/#/gantt
    %%
    axisFormat %H:%M
    %% Give timestamps as unix timestamps (ms)
    dateFormat x
"""
    )

    for runlog in load_sorted_runlogs():
        f.write(f"""    section {runlog_taskname(runlog)}\n""")

        # render failed tasks in red
        if runlog["out.status"] == "FAILURE":
            modifier = "crit"
        else:
            modifier = ""

        f.write(
            f"""    {runlog_duration_text(runlog)} - {runlog["out.status"]} :{modifier}, """
            f"""{runlog["out.timing.start_ts"] // 1000000}, """
            f"""{runlog["out.timing.end_ts"] // 1000000} \n"""
        )

print("Done")
