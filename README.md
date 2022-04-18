# `pynb-dag-runner` Python library
**py_dag_runner** is a open source Python library for running pipelines of Jupter notebooks. Main features:

### Tasks are executed in parallel using the [Ray](https://www.ray.io/) framework
 - With a DAG one can define in which order tasks should run.
 - Tasks run in parallel (subject to DAG constraints).
 - Currently `pynb-dag-runner` only support single node Ray clusters.

### Pipeline outputs are saved using the [OpenTelemetry](https://opentelemetry.io/) standard
- Pipeline telemetry is emitted using the OpenTelemetry (open) standard. This includes:
  - Pipeline configuration.
  - Parameters used to trigger tasks/notebooks.
  - Any logged images, metrics, rendered notebooks, or even models.
  - Timing and other outcomes (eg., did a task fail, succeed, timeout, any retries).
- Effectively, this means that after a pipeline has run all relevant information can be stored as one json file. Alternatively, this can be expanded into a directory structure, or the events can be redirected into any service that support OpenTelemetry (span events).
- Use of OpenTelemetry is possible since Ray supports OpenTelmetry.

### Reporting using custom static version of [mlflow](https://mlflow.org/)
- OpenTelemetry files emitted from pipeline runs can be converted into a static website (built using a custom version of mlflow).
- See [demo hosted on Github Pages](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/).

----

The advantage of this approach is that pipelines can be be run without any cloud infrastructure except a Github account (see below).

The main limitation compared to other options, is that there is no real-time monitoring.

See [docs/NOTES.md](docs/NOTES.md) for further comments about the implementation and dependencies.

## ML Ops demo pipeline
The below shows an example ML pipeline that trains a model for detecting hand written digits. Moreover, it explores how the size of the training set influences model performance:

![task-dependencies.png](./assets/task-dependencies.png)

Using `pynb-dag-runner`, this pipeline is implemented in the below repo
- [mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline)

This repo has Github Actions conifgured to run the pipeline daily (on a Github hosted runner), and results are saved as build artifacts.

Past pipeline results can be inspected from the (static) mlflow site:
- [https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)

This is built using the custom clone of mlflow [pynb-dag-runner/mlflow](https://github.com/pynb-dag-runner/mlflow).

(**Caveat:** The demo pipeline is currently being refactored. All steps have not yet refactored, and are not seen in the above links.)

This example pipeline setup is illustrated below:

```mermaid
graph BT;
%%graph LR;

subgraph "Laptop"
    laptop[Local pipeline <br> development]
end

subgraph "Github services"
       git_repo[Git repo: Notebooks, unit tests, DAG definitions]

       subgraph "Github hosted actions runner (2 CPU, 7 GB RM, 14 GB SSD)"
          subgraph "Ray cluster"
            tests[Tests <br> Unit, Type checks, Formatting]
            run_pipeline["Run pynb-dag-runner pipeline"]
            otel_logs["OpenTelemetry logged spans<br>(inc. logged notebooks, images, metrics, and artifacts)"]
          end
       end

       subgraph "GitHub Build artifacts"
          build_artifacts[Pipeline OpenTelemetry outputs persisted for max 90 days]
       end

       subgraph "Github Pages"
          subgraph "Data pipeline monitoring and reporting"
             web_static_mlflow[Custom statically built version of ML Flow for hosting static data]
             web_static_mlflow_data[Logged notebooks, images, metrics]
             web_static_mlflow_logs[Pipeline run logs]
          end
    end
end

laptop ---->|git push| git_repo
git_repo ---->|GHA: trigger on PR | tests
git_repo ---->|GHA: trigger on PR, on schedule | run_pipeline
run_pipeline --> otel_logs
otel_logs ---> |GHA| build_artifacts

build_artifacts --->|GHA| web_static_mlflow

web_static_mlflow_data -.- web_static_mlflow
web_static_mlflow_logs -.- web_static_mlflow
Internet --> web_static_mlflow
```

## Local development
Development environments are dockerized, and makefile:s should be available for most common tasks. See:

- [docs/LOCAL_UI_DEV.md](docs/LOCAL_UI_DEV.md): local development for developing mlflow frontend with static data

- [docs/LOCAL_DEV.md](docs/LOCAL_DEV.md): local development of main `pynb_dag_runner` Python library

The Github actions in the `pynb-dag-runner` and `mnist-digits-demo-pipeline` repos can also be useful since they show how various tests (unit, static code analysis) and pipelines are run in docker.

Instructions assume linux-based setup (eg. ubuntu), but should also work on macs with some changes (or at least on non-M1 ones).

## Contact
Please note that this is experimental and 🚧🚧🚧.

A motivation for this work is to make it easier to set up and work together (on pipelines). If you would like to discuss an idea or question, please raise an [issue](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline/issues) or contact me via email.

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
