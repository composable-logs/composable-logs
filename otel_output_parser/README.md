# ci-utils
Utils/scripts for running pynb-dag-runner in Github actions and processing output run logs

## Local setup
(Tested on Ubuntu system)

### Taskfile
This project uses [Taskfile](https://taskfile.dev/) (a yaml-based tool similar to makefile)
for common project task definitions.

### Installation
For install instructions, see `https://taskfile.dev/#/installation`.

Eg., if `~/.local/bin` is in `$PATH`, Taskfile can be installed for the user as:
```bash
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin
```

Verify that the Taskfile is installed by running `task --version`.

### List of project tasks
After taskfile is installed, running `task --list` shows a list of available tasks:

```
task: Available tasks for this project:
* code:black: 		Check that code is Black formatted (black)
* code:mypy: 		Check static mypy type checks (mypy)
* code:pytest: 		Run unit tests (pytest)
* code:watch-all: 	Watch all tests in tmux session
* docker:build: 	Build project Docker image
* docker:run: 		Run bash command in project Docker image (interactive if IS_INTERACTIVE=true)
* docker:shell: 	Start an interactive shell in project Docker image
```

Individual tasks are run as eg `task docker:build`.

## VS Code development
- Set up VS Code (TBD)

- Inside Docker dev-container, the `code:watch-all` task can be run using
  "Ctrl + Shift + P" -> "Run Task" and select the "code:watch-all" task.
