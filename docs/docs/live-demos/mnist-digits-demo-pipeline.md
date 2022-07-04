---
hide:
  #- navigation
  - toc
---

A goal of `pynb-dag-runner` is to make it easy to deploy data/ml pipelines with
no (or with minimal) cloud infrastructure.

Currently there is one live demo pipeline `mnist-digits-demo-pipeline` illustrating this in practice.

## `mnist-digits-demo-pipeline`

This pipeline trains a model for recognizing hand written digits from a
toy MNIST data set included in sklearn library.

- Github: [https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline)
- Public experiment tracker: [https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)


### Pipeline task DAG

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


### Main features

- [x] The pipeline is scheduled to run every day and for all pull requests to the repo.
- [x] The pipeline runs without any cloud infrastructure, and
      pipeline uses only services provided with a (free personal)
      Github account:
       - Use **Github Actions** for compute resources, and for orchestration.
       - Use **Github Build Artifacts** for storing pipeline run logs.
       - Use **Github Pages** for pipeline/task tracker and ML-experiment tracker
         (hosted as a static site and built from pipeline run logs).
- [x] Outcomes of pipeline runs (both scheduled- and PR- pipeline runs) can be inspected in the
      Experiment tracker (see above, this is hosted as a static website and build using a fork of MLFlow).
- [x] This setup does not require any databases or tracking servers (eg. for task execution,
      like Airflow) or for experiment tracking (eg. ML tracking server, like an MLFlow backend).

This setup could be used to run public open source -- open data pipelines using only a free personal Github account.

### Architecture and use of Github services
``` mermaid
graph BT;

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
### Run locally

``` bash
git clone --recurse-submodules git@github.com:pynb-dag-runner/mnist-digits-demo-pipeline.git

cd mnist-digits-demo-pipeline
make docker-build-all

make clean

# run
# - unit, type and linter tests
# - pipeline in dev-mode
make test-and-run-pipeline
```

**Notes**

- After the pipeline has run, results and outputs can be inspected in the `pipeline-outputs`-directory.
- For further details, please see the demo pipeline repo, [link](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline).
- In the above, `make test-and-run-pipeline` is currently slow.
  Please see [Next steps](/next-steps/) for plans to address this.

### Notes and possible limitations
- Compute resources provided for free by Github are limited. This could be improved with a more powerful
  self-hosted runner. Since `pynb-dag-runner` is built on [Ray framework](https://www.ray.io/), it can
  also run tasks in parallel provided (provided compute resources).
- Pipeline run logs are published as build artifacts to a public repo.
    - If any secrets are used in the pipeline, this require special care.
    - For public Github repos, build artifacts have maximum retention period of 90 days.
- The Experiment tracker UI is public website. Making the website private requires a premium [Github subscription](https://docs.github.com/en/enterprise-cloud@latest/pages/getting-started-with-github-pages/changing-the-visibility-of-your-github-pages-site).

These limitations could be addressed by introducing cloud infrastructure and customization.
On the other hand, in that case there are also many other options.
