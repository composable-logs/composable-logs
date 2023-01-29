---
hide:
  #- navigation
  - toc
---

One goal of Composable Logs is to make it easy to deploy data/ml pipelines with no (or with minimal) cloud infrastructure.

Currently there is one live demo `mnist-digits-demo-pipeline` illustrating this in practice.

## Demo ML training pipeline: `mnist-digits-demo-pipeline`

**Public experiment tracker (hosted on Github Pages):** [https://composable-logs.github.io/mnist-digits-demo-pipeline<sup><sup><sub>:material-launch:</sub></sup></sup>](https://composable-logs.github.io/mnist-digits-demo-pipeline/)

<figure markdown>
  [![screenshot-task-list.png](screenshot-task-list.png){ width="800"}](https://composable-logs.github.io/mnist-digits-demo-pipeline/)
</figure>

**Code repo:** [https://github.com/composable-logs/mnist-digits-demo-pipeline<sup><sup><sub>:material-launch:</sub></sup></sup>](https://github.com/composable-logs/mnist-digits-demo-pipeline)

### Main features

- [x] The pipeline is scheduled to run every day and for all pull requests to the repo.
- [x] The pipeline runs without any cloud infrastructure, and pipeline uses only services provided with a (free personal) public Github account:
       - Use **Github Actions** for compute resources, and for orchestration.
       - Use **Github Build Artifacts** for storing pipeline run logs.
       - Use **Github Pages** for pipeline/task tracker and ML-experiment tracker
         (hosted as a static site and built from pipeline run logs).
- [x] Outcomes of pipeline runs (both scheduled- and PR- pipeline runs) can be inspected in the Experiment tracker (see above, this is hosted as a static website and build using a fork of MLFlow).

This setup could be used to run public open source -- open data pipelines using only a free personal Github account.
### Task DAG

This pipeline trains a model for recognizing hand written digits from a toy MNIST data set included in sklearn library.

As shown below, the pipeline explores how performance depends on the size of the training set.

``` mermaid

graph LR
    %% Mermaid input file for drawing task dependencies
    %% See https://mermaid-js.github.io/mermaid
    %%
    %%
    %% The below is slightly modified from output of local run.
    %%
    TASK_SPAN_ID_0xc93b72d91fc8351f["<b>ingest.py</b> <br />task.max_nr_retries=15<br />task.num_cpus=1<br />task.timeout_s=10"]
    TASK_SPAN_ID_0xd73557eb405ac5b2["<b>eda.py</b> <br />task.max_nr_retries=1<br />task.num_cpus=1"]
    TASK_SPAN_ID_0x276bb1087b500b48["<b>split-train-test.py</b> <br />task.max_nr_retries=1<br />task.num_cpus=1<br />task.train_test_ratio=0.7"]
    TASK_SPAN_ID_0x454402735adad972["<b>train-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=500<br />task.num_cpus=1"]
    TASK_SPAN_ID_0xaede930460f66e00["<b>train-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=400<br />task.num_cpus=1"]
    TASK_SPAN_ID_0x18b163a530a74e8b["<b>train-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=600<br />task.num_cpus=1"]
    TASK_SPAN_ID_0xf46555d79f8c24f8["<b>benchmark-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=400<br />task.num_cpus=1"]
    TASK_SPAN_ID_0xaf10d6618aa0b457["<b>benchmark-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=600<br />task.num_cpus=1"]
    TASK_SPAN_ID_0xefbc89de2baee79f["<b>benchmark-model.py</b> <br />task.max_nr_retries=1<br />task.nr_train_images=500<br />task.num_cpus=1"]
    TASK_SPAN_ID_0x35f37ef6c7ccaf05["<b>summary.py</b> <br />task.max_nr_retries=1<br />task.num_cpus=1"]
    TASK_SPAN_ID_0x454402735adad972 --> TASK_SPAN_ID_0xefbc89de2baee79f
    TASK_SPAN_ID_0x18b163a530a74e8b --> TASK_SPAN_ID_0xaf10d6618aa0b457
    TASK_SPAN_ID_0x276bb1087b500b48 --> TASK_SPAN_ID_0xaede930460f66e00
    TASK_SPAN_ID_0x276bb1087b500b48 --> TASK_SPAN_ID_0x454402735adad972
    TASK_SPAN_ID_0xf46555d79f8c24f8 --> TASK_SPAN_ID_0x35f37ef6c7ccaf05
    TASK_SPAN_ID_0xaede930460f66e00 --> TASK_SPAN_ID_0xf46555d79f8c24f8
    TASK_SPAN_ID_0xaf10d6618aa0b457 --> TASK_SPAN_ID_0x35f37ef6c7ccaf05
    TASK_SPAN_ID_0xc93b72d91fc8351f --> TASK_SPAN_ID_0x276bb1087b500b48
    TASK_SPAN_ID_0xc93b72d91fc8351f --> TASK_SPAN_ID_0xd73557eb405ac5b2
    TASK_SPAN_ID_0x276bb1087b500b48 --> TASK_SPAN_ID_0x18b163a530a74e8b
    TASK_SPAN_ID_0xefbc89de2baee79f --> TASK_SPAN_ID_0x35f37ef6c7ccaf05
```

### Architecture and use of Github services

A special feature of the below architecture is that each run of the pipeline can be executed serverless using ephemeral compute resources.
So, after a pipeline has run, we only need to persist the (OpenTelemetry) logs of that run, and those are stored as one immutable JSON-file per pipeline run.

In other words, the architecture does not include any tracking servers that need to run 24/7 (eg. for task execution, like Airflow) or for experiment tracking (eg. for ML tracking, like an MLFlow backend).
In particular, the architecture does not include any databases.

``` mermaid
graph BT;

subgraph "Laptop"
    laptop[Local development]
end

subgraph "Github services"
       git_repo[Git repo: Notebooks, unit tests, DAG definitions]

       subgraph "Github hosted actions runner (2 CPU, 7 GB RM, 14 GB SSD)"
          subgraph "Ray cluster"
            tests[Tests <br> Unit, Type checks, Formatting]
            run_pipeline["Run composable-logs workflow"]
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

!!! info
    The demo does not deploy the trained model as an REST API, but the trained model is saved in ONNX format. Thus, it could eg. be included into a website.

#### Notes and possible limitations
- Compute resources provided for free by Github are limited. This could be improved with a more powerful self-hosted runner.
- The logs from the demo training pipeline are published as build artifacts to a public repo.
    - Thus, if any secrets are used in the pipeline, this require special care.
    - For public Github repos, build artifacts have maximum retention period of 90 days.
- The Experiment tracker UI is deployed as a public website. Making this private is possible. Either with a custom service. Github Pages can also be made private with a premium [Github subscription](https://docs.github.com/en/enterprise-cloud@latest/pages/getting-started-with-github-pages/changing-the-visibility-of-your-github-pages-site).

These limitations could be addressed by introducing cloud infrastructure and customization.

### Run locally

#### From command line
The training pipeline can be run either from the command line (as done in CI automation):

``` bash
git clone --recurse-submodules git@github.com:composable-logs/mnist-digits-demo-pipeline.git

cd mnist-digits-demo-pipeline
make build-docker-images

make clean

# run
# - unit, type and linter tests
# - pipeline in dev-mode
make test-and-run-pipeline
```

#### VS Code setup
For development, the training workflow (and associated unit tests and type checking) can run in watch mode in VS Code. See the repo for further [instructions](https://github.com/composable-logs/mnist-digits-demo-pipeline).

*/insert screenshot/*

###### Notes
- After the training pipeline has run, results and outputs can be inspected in the `pipeline-outputs`-directory.
- For further details, please see the [mnist-digits-demo-pipeline](https://github.com/composable-logs/mnist-digits-demo-pipeline) repo.
