# `otel_output_parser`
Utils/scripts for:
- parsing OpenTelemetry logs emitted from pynb-dag-runner
- download Build Artifacts from Github actions
- scripts for converting OpenTelemetry logs into a static MLFlow website.
### List of project tasks

```bash
# run all tests in (w tmuxinator)
make docker-tmux-watch-all-tests

# Run pytests (optionally in watch mode, with filtering)
make docker-pytest [WATCH_MODE=1] [PYTEST_FILTER="test_name_search_string"]
```

## VS Code development
- Set up VS Code (TBD)

- Inside Docker dev-container, the `code:watch-all` task can be run using
  "Ctrl + Shift + P" -> "Run Task" and select the "code:watch-all" task.
