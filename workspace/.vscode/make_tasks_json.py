"""
This script create the .vscode/tasks.json file with dev task definitions.

The tasks typically watch all Python files for a component and run unit (pytest),
type (mypy) and black format testing on the component whenever a Python file is
saved. However, the tasks and commands are configurable, see below.

Note: the list of files to watch is determined when the a task is started. So, if a
new file is created after that, the file will not be watched by the entr tool.
"""

import json
from typing import Dict


def make_task_dict(
    component: str,
    component_relative_path: str,
    task_desc: str,
    make_command: str,
):
    """
    Create task-dict for an individual task.
    """
    return {
        "label": f"({component}: {task_desc})",
        "type": "shell",
        "command": f"""(cd {component_relative_path}; find . | grep ".py" | entr {make_command})""",
        "group": {"kind": "build", "isDefault": True},
        "presentation": {
            "reveal": "always",
            "panel": "new",
            # terminals in the group are shown side-by-side
            "group": f"terminal-group-id={component}",
        },
        "problemMatcher": [],
    }


def make_component_tasks(
    component_name: str,
    component_relative_path: str,
    task_commands: Dict[str, str],
):
    individual_tasks = [
        make_task_dict(component_name, component_relative_path, task_desc, command)
        for task_desc, command in task_commands.items()
    ]

    # Add task to start all tasks (for this component) side by side in one terminal
    return [
        {
            "label": f"{component_name} - watch and run all tasks",
            "dependsOn": [task["label"] for task in individual_tasks],
            "problemMatcher": [],
        }
    ] + individual_tasks


def write_tasks_file_dict(output_file, tasks):
    with open(output_file, "wt") as f:
        f.write("// This file is dynamically created -- do not edit.\n")
        f.write("//\n")
        f.write("// The below are task definitions for use by VS Code editor.\n")
        f.write("//\n")
        f.write("// Run tasks in VS Code by pressing Ctrl + Shift + P,\n")
        f.write("// select 'Tasks: Run task' and choose the task to run.\n")
        f.write("//\n")
        f.write("// See, https://code.visualstudio.com/docs/editor/tasks\n")
        f.write(
            json.dumps(
                {
                    "version": "2.0.0",
                    "tasks": tasks,
                },
                indent=4,
            )
        )
        f.write("\n")


if __name__ == "__main__":
    print(" - Updating tasks.json for pynb_dag_runner library ...")

    write_tasks_file_dict(
        output_file="tasks.json",
        tasks=make_component_tasks(
            component_name="pynb_dag_runner library",
            component_relative_path="pynb_dag_runner",
            task_commands={
                "run unit tests": "make test-pytest",
                "run static code analysis": "make test-mypy",
                "check code is linted": "make test-black",
            },
        ),
    )

    print(" - Done")
