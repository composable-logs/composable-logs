# ci-utils
Utils/scripts for running pynb-dag-runner in Github actions and processing output run logs


## Local setup

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
Run `task --list` for list of tasks.

```
task: Available tasks for this project:
* code:black: 	Check that code is Black formatted (black)
* code:mypy: 	Check static mypy type checks (mypy)
* code:pytest: 	Run unit tests (pytest)
* docker:build: Build project Docker image
* docker:run: 	Run a non-interactive command inside project Docker image
* docker:shell: Start an interactive shell in project Docker image
```

Individual tasks are run as eg `task docker:build`.
## License

(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
