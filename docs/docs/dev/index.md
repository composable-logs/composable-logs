### [pynb-dag-runner/mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline)
 - Repo for the mnist demo pipeline (Python, notebooks, GHA definitions)

### [pynb-dag-runner/pynb-dag-runner](https://github.com/pynb-dag-runner/pynb-dag-runner)
 - Repo for the main library (Python, Ray, OpenTelemetry). This is used when running the demo pipeline.

### [pynb-dag-runner/mlflow](https://github.com/pynb-dag-runner/mlflow).
 - A slightly modified version of MLFlow, that can build static MLFlow-like sites where all metadata is included in the front end.
 - The demo site linked [above](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline) is built using this repo.

---

Development environments are dockerized, and makefile:s should be available for most common tasks.

Inspecting the Github actions in the `pynb-dag-runner` and `mnist-digits-demo-pipeline` repos can also be useful since they show how various tests (unit, static code analysis) and pipelines are run in docker.

Instructions assume linux-based setup (eg. ubuntu), but should also work on macs with some changes (or at least on non-M1 ones).
