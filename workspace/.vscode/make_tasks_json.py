"""
Script to create .vscode/tasks.json (VS Code tasks).

- Update task definitions by manually running

    python3 make_tasks_json.py

(in the ./workspace/.vscode/ directory).

- In VS Code, the tasks can be started by

    Ctrl+Shift+P -> "Tasks: Run task" -> select task to run

- The dev-tasks run the make-commands (see below) in watch mode and they are triggered
  whenever a component .py file is saved.

- Documentation: https://code.visualstudio.com/docs/editor/tasks

"""

import json


def make_task_dict(
    component: str,
    component_relative_path: str,
    task_desc: str,
    make_command: str,
):
    """
    Create dict for an individual task
    """
    return {
        "label": f"({component}: {task_desc})",
        "type": "shell",
        "command": f"""(cd {component_relative_path}; find . | grep ".py" | entr {make_command})""",
        "group": {"kind": "build", "isDefault": True},
        "presentation": {
            "reveal": "always",
            "panel": "new",
            # create component-based id for which tasks should be shown side-by-side
            "group": f"{component} (terminal-group-id)",
        },
        "problemMatcher": [],
    }


def make_component_tasks(component_name: str, component_relative_path: str):
    dev_tasks = [
        ("run unit tests", "make test-pytest"),
        ("mypy static code analysis", "make test-mypy"),
        ("black format code check", "make test-black"),
    ]

    individual_tasks = [
        make_task_dict(component_name, component_relative_path, task_desc, command)
        for task_desc, command in dev_tasks
    ]

    # Add single task to start all three tasks (for this component) in split terminal
    return [
        {
            "label": f"{component_name} - watch and run all tasks",
            "dependsOn": [task["label"] for task in individual_tasks],
            "problemMatcher": [],
        }
    ] + individual_tasks


with open("tasks.json", "w") as f:
    f.write("// Dynamically created tasks.json -- do not edit.\n")
    f.write("// See make_tasks_json.py\n")

    f.write(
        json.dumps(
            {
                "version": "2.0.0",
                "tasks": (
                    make_component_tasks(
                        component_name="pynb_dag_runner library",
                        component_relative_path="pynb_dag_runner",
                    )
                ),
            },
            indent=4,
        )
    )
    f.write("\n")
