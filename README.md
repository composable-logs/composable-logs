# `pynb-dag-runner` Python library

🚧 🚧 WIP 🚧 🚧

## Development setup

The below VS Code/Docker based setup has been tested on Linux (Kubuntu), but should also work on MacOS with minor changes. For more details, see VS Code remote development [link](https://code.visualstudio.com/docs/remote/remote-overview).

### In VS Code
- Open the root of this repository in VS Code.
- In the repo root run `make docker-build-all`.
- Ensure that the extension "Remote - Containers" (`ms-vscode-remote.remote-containers` published by Microsoft) is installed.
- Press the `><`-button in the lower left VS Code window corner. Select "Reopen in container".
- Inside container ensure that the "Python" extension is installed (`ms-python.python` also published by Microsoft) if it is not automatically installed. When installed and enabled, the lower row will show the Python version in use inside the container.
- To start tests (unit tests, black, and mypy) in watch mode, start the task "pynb_dag_runner library - watch and run all tasks" (`Ctrl` + `Shift` + `P`).

## Run tests and build Python wheel file

```bash
make docker-build-all
make [test|build|clean]
```

## License

MIT, see [LICENSE.md](./LICENSE.md).
