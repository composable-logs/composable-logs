<div align="center">

  <!-- need an empty line above: https://stackoverflow.com/a/70293384 -->
  <a href="https://github.com/composable-logs/composable-logs/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml">![ci/cd: publish snapshot to PyPI](https://github.com/composable-logs/composable-logs/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml/badge.svg)</a>
  <a href="https://badge.fury.io/py/composable-logs"><img src="https://badge.fury.io/py/composable-logs.svg" alt="PyPI version" height="18"></a>
  <a href="https://github.com/composable-logs/composable-logs/blob/main/LICENSE.md">![license=mit](https://img.shields.io/badge/license-MIT-blue)</a>
  <a href="https://github.com/composable-logs/composable-logs/issues/new">![Ideas and feedback=welcome](https://img.shields.io/badge/Ideas%20%26%20feedback-welcome-green)</a>

</div>

# `Composable Logs`

> ***Composable Logs** is a Python library to run ML/data workflows on stateless compute infrastructure (that may be ephemeral or serverless).*

In particular, Composable Logs supports ML experiment tracking without a dedicated tracking server (and database) to record ML metrics, models or artifacts.
Instead, these are emitted using the **[OpenTelemetry standard](https://opentelemetry.io/)** for logging.
This is an open standard in software engineering with growing support.

For example, log events emitted from Composable Logs can be directed to a JSON-file, or sent to any log storage supporting OpenTelemetry (span) events. In either case, this means that one does not need a separate tracking service only for ML experiments.

Composable Logs uses the **[Ray framework](https://www.ray.io/ray-core)** for parallel task execution.

### For more details:

#### Documentation and architecture
- **https://composable-logs.github.io/composable-logs**

#### Live demo
- Using Composable Logs one can run a ML training pipeline using only a free Github account. This uses:
   - **Github actions**: trigger the ML pipeline daily and for each PR.
   - **Build artifacts**: to store OpenTelemetry logs of past runs.
   - **Github Pages**: to host static website for reporting on past runs.

   The static website is rebuilt after each pipeline run (by extracting relevant data from past OpenTelemetry logs). This uses a fork of MLFlow that can be deployed as a static website, https://github.com/composable-logs/mlflow.

  [![Screenshot](https://composable-logs.github.io/composable-logs/live-demos/mnist-digits-demo-pipeline/screenshot-task-list.png)](https://composable-logs.github.io/mnist-digits-demo-pipeline/)

- Codes for pipeline (MIT): https://github.com/composable-logs/mnist-digits-demo-pipeline

#### Public roadmap and planning
- https://github.com/orgs/composable-logs/projects/2/views/3

#### Install via PyPI

##### Latest release
- `pip install composable-logs`
- https://pypi.org/project/composable-logs

##### Snapshot of latest commit to main branch
- `pip install composable-logs-snapshot`
- https://pypi.org/project/composable-logs-snapshot

---

Any feedback/ideas welcome!

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).

(Note: As of 1/2023 this project was renamed from `pynb-dag-runner` to `composable-logs`.)
