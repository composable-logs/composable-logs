<div align="center">

  <!-- need an empty line above: https://stackoverflow.com/a/70293384 -->
  <a href="https://github.com/composable-logs/composable-logs/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml">![ci/cd: publish snapshot to PyPI](https://github.com/composable-logs/composable-logs/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml/badge.svg)</a>
  <a href="https://badge.fury.io/py/composable-logs"><img src="https://badge.fury.io/py/composable-logs.svg" alt="PyPI version" height="18"></a>
  <a href="https://github.com/composable-logs/composable-logs/blob/main/LICENSE.md">![license=mit](https://img.shields.io/badge/license-MIT-blue)</a>
  <a href="https://github.com/composable-logs/composable-logs/issues/new">![Ideas and feedback=welcome](https://img.shields.io/badge/Ideas%20%26%20feedback-welcome-green)</a>

</div>

# `composable-logs`
# Composable Logs
# `Composable Logs`
# `Composable logs`

***Composable Logs** is a Python library that can run ML/data pipelines on stateless compute infrastructure (that may be ephemeral or serverless).*

This means that Composable Logs does not need a tracking server (or database) to record task outcomes (like logged ML metrics, models, artifacts).
Instead, pipeline outputs are emitted using the [OpenTelemetry standard](https://opentelemetry.io/).
Since structured logs can be directed to a file (as one option), this can be used to run pipelines on limited or no cloud infrastructure;
after pipeline execution one only needs to preserve the structured logs.

### Documentation and architecture
- **https://composable-logs.github.io/composable-logs**

### Demo
- The below shows a demo ML training pipeline that uses only Github infrastructure (that is: Github actions for compute; Build artifacts for storage; and Github Pages for reporting). This uses Composable Logs and a fork of MLFlow that can be deployed as a static website (see, https://github.com/composable-logs/mlflow).

  [![Screenshot](https://composable-logs.github.io/composable-logs/live-demos/mnist-digits-demo-pipeline/screenshot-task-list.png)](https://composable-logs.github.io/mnist-digits-demo-pipeline/)

- Codes for pipeline (MIT): https://github.com/composable-logs/mnist-digits-demo-pipeline

### Roadmap and project planning
- https://github.com/orgs/composable-logs/projects/2/views/3

### Install via PyPI

#### Latest release
- `pip install composable-logs`
- https://pypi.org/project/composable-logs

#### Snapshot of latest commit to main branch
- `pip install composable-logs-snapshot`
- https://pypi.org/project/composable-logs-snapshot

---

Any feedback/ideas welcome!

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
