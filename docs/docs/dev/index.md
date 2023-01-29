### [composable-logs/mnist-digits-demo-pipeline](https://github.com/composable-logs/mnist-digits-demo-pipeline)
 - Repo for the mnist demo training pipeline.
 - A public task and experiment tracker for this pipeline is hosted on Github Pages:
<figure markdown>
  [![screenshot-task-list.png](../live-demos/mnist-digits-demo-pipeline/screenshot-task-list.png){ width="600"}](https://composable-logs.github.io/mnist-digits-demo-pipeline/)
  <figcaption>
  <b><a href="https://composable-logs.github.io/mnist-digits-demo-pipeline/">
  https://composable-logs.github.io/mnist-digits-demo-pipeline/
  </a></b>
  </figcaption>
</figure>


### [composable-logs/composable-logs](https://github.com/composable-logs/composable-logs) (Python)
 - Repo for the main library (Python, Ray, OpenTelemetry). This is used when running the demo pipeline.
 - Dependency for the above demo pipeline.
 - Install via PyPI:
     - `pip install composable-logs` (latest release, [PyPI link<sup><sup><sub>:material-launch:</sub></sup></sup>](https://pypi.org/project/composable-logs))
     - `pip install composable-logs-snapshot` (dev-release, latest commit to main branch, [PyPI link<sup><sup><sub>:material-launch:</sub></sup></sup>](https://pypi.org/project/composable-logs-snapshot))

### [composable-logs/mlflow](https://github.com/composable-logs/mlflow) (Javascript)
 - A slightly modified version of MLFlow, that can build static MLFlow-like sites where all metadata is included in the front end.
 - Dependency for the above demo pipeline.
 - The static assets are made available as a Python package via PyPI:
     - Install as `pip install composable-logs-webui --target <install dir>` (latest release, [PyPI link<sup><sup><sub>:material-launch:</sub></sup></sup>](https://pypi.org/project/composable-logs-webui)). After installation the assets will be in the `assets` directory.

---

Development environments are dockerized, and makefile:s should be available for most common tasks.

Inspecting the Github actions in the `composable-logs` and `mnist-digits-demo-pipeline` repos can also be useful since they show how various tests (unit, static code analysis) and pipelines are run in docker.

Instructions assume linux-based setup (eg. Ubuntu), but should also work on macs with some changes (or at least on non-M1 ones).
