<div align="center">

  <!-- need an empty line above: https://stackoverflow.com/a/70293384 -->
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml">![ci/cd: publish snapshot to PyPI](https://github.com/pynb-dag-runner/pynb-dag-runner/actions/workflows/cicd_publish-pypi-package-dev-snapshot.yml/badge.svg)</a>
  <a href="https://badge.fury.io/py/pynb-dag-runner"><img src="https://badge.fury.io/py/pynb-dag-runner.svg" alt="PyPI version" height="18"></a>
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/blob/main/LICENSE.md">![license=mit](https://img.shields.io/badge/license-MIT-blue)</a>
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/issues/new">![Ideas and feedback=welcome](https://img.shields.io/badge/Ideas%20%26%20feedback-welcome-green)</a>

</div>

---

**`pynb-dag-runner` is a Python library that can run ML/data pipelines on stateless compute infrastructure (that may be ephemeral or serverless).**

This means that `pynb-dag-runner` does not need a tracking server (or database) to record task outcomes/logged ML metrics/artifacts.
Instead the key data during a pipeline run are emitted using the [OpenTelemetry standard](https://opentelemetry.io/).

Since structured logs can be directed to a file (as one option), this can be used to run pipelines on limited or no cloud infrastructure.
Eg. one can use Github actions for compute and Github pages for reporting. See below for a demo of this.


### Links

#### Documentation and architecture
- **https://pynb-dag-runner.github.io/pynb-dag-runner**

#### Install via PyPI
- `pip install pynb-dag-runner` (latest release)
- `pip install pynb-dag-runner-snapshot` (dev-release, latest commit to main branch)

#### Demo pipeline with task and experiment tracker hosted as static page on Github Pages
- Demo: https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/

  ![Screenshot](https://pynb-dag-runner.github.io/pynb-dag-runner/live-demos/mnist-digits-demo-pipeline/screenshot-task-list.png)

- Repo (MIT): https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline

---

Any feedback/ideas welcome!

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
