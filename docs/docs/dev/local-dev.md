## Set up local dev environment for local development the main `pynb-dag-runner`library.

### In VS Code
- Open the root of the `pynb-dag-runner` repository in VS Code.
- In the repo root run `make build-docker-images`.
- Ensure that the extension "Remote - Containers" (`ms-vscode-remote.remote-containers` published by Microsoft) is installed.
- Press the `><`-button in the lower left VS Code window corner. Select "Reopen in container".
- Inside container ensure that the "Python" extension is installed (`ms-python.python` also published by Microsoft) if it is not automatically installed. When installed and enabled, the lower row will show the Python version in use inside the container.
- To start tests (unit tests, black, and mypy) in watch mode, start the task "pynb_dag_runner library - watch and run all tasks" (`Ctrl` + `Shift` + `P`).

For more details, see VS Code remote development, [docs<sup><sup><sub>:material-launch:</sub></sup></sup>](https://code.visualstudio.com/docs/remote/remote-overview).

### Run tests and build Python wheel file

Common development tasks are found in the root `makefile`.

```bash
# --- setup ---
# this will build three Docker images. For cd, ci and local-development.
make build-docker-images

# --- testing ---
# run all unit tests in watch mode
make in-dev-docker/watch-pytest

# run selected unit tests in watch mode.
make in-dev-docker/watch-pytest PYTEST_FILTER="version"

# Start tmux session to watch
#  - black formatting
#  - static type cheks
#  - and unit tests [optionally filtered as above]
make in-dev-docker/tmux-watch-all-tests [PYTEST_FILTER="version"]

# --- build package locally ---
# TODO, see the in-ci-docker/build task in the root makefile

# --- clean up ---
make clean
```
