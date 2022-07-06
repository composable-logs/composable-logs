---
hide:
  #- navigation
  - toc
---

**`pynb-dag-runner`** is an open source Python framework for running Python (notebook) ML/data pipelines.

A main feature of `pynb-dag-runner` is that pipelines can execute on stateless compute infrastructure (that may be ephemeral, serverless).
So, a 24/7 running database that records past runs or logged metrics is not needed.
Rather, when `pynb-dag-runner` executes a pipeline, all key events (and logged artifacts) are emitted using the [OpenTelemetry standard](https://opentelemetry.io/).
Thus, after a pipeline has completed, a complete immutable record of the run can be persisted as a JSON file to a data lake (as one option).

For reporting and experiment tracking, `pynb-dag-runner` can currently convert pipeline logs into a static website, see the [example website](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/) generated from daily runs of the `mnist-digits-demo-pipeline` [example pipeline](/live-demos/mnist-digits-demo-pipeline/).
This is done with a modified version of the MLFlow UI.

A main motivation for developing `pynb-dag-runner` is to have a framework to run pipelines on limited or no cloud infrastructure.
For example, the demo ML training pipeline linked above runs using only services provided with a (free, personal) Github account, and the UI is hosted as a static website.

---

##### Overall architecture for `pynb-dag-runner` pipeline runs

``` mermaid
graph TB;

subgraph " "
Git[Git repository]
Developer[Developer, <br />local development]

Developer --> Git
  subgraph "Stateless execution"
    subgraph "<b>Execution driver</b> (pynb-dag-runner)"
        Code[Codes]
    end

    subgraph "<b>Execution backend</b> (Ray cluster)"
        run_1[Pipeline run 1]
        run_2[Pipeline run 2]
        run_3[Pipeline run 3]
    end

    Code --> run_1
    Code --> run_2
    Code --> run_3
  end

  subgraph "<b>Storage for persisted logs</b> <br/> (eg. data lake, OpenTelemetry DB, Github build artifact)"
    logs_1[Logs for run 1]
    logs_2[Logs for run 2]
    logs_3[Logs for run 3]
  end

  subgraph "<b>Reporting and UI</b>"
    direction TB
    ui_data[Processed data]
    ui_website[Website with past runs <br/> using modified version of MLFlow]
    ui_data --> ui_website
  end

  run_1 --> logs_1
  run_2 -->|After a run has<br/>completed, persist <br/>OpenTelemetry<br/>logs.| logs_2
  run_3 --> logs_3

  logs_1 --> ui_data
  logs_2 -->|Convert logged <br/>OpenTelemetry data<br/> into UI-friendly format| ui_data
  logs_3 --> ui_data

  ui_website ---> Developer
  Git --> Code
end
```

!!! info
    The task execution framework for `pynb-dag-runner` is built using the [Ray framework](https://www.ray.io/ray-core), and pipeline tasks can run in parallel.
    Ray does have support for larger clusters (with support for public clouds and Kubernetes, [details](https://docs.ray.io/en/latest/cluster/deploy.html)).
    However, execution on multi-node Ray clusters is not supported by `pynb-dag-runner` (at least yet).

!!! info
    On use of OpenTelemetry: When pipelines are logged with OpenTelemetry, they could potentially be correlated with other (system) metrics. Eg.

    - To troubleshoot a failed data ingestion task, it can be useful to view its network input/output.
    - Before deploying a long running ML-training job, it might be useful to monitor GPU/CPU loads. Eg. is it 10% or 90%.

    Details on this would need to be investigated, tbd. Is this feasible?


## Use cases and scope

- `pynb-dag-runner` can currently run public pipelines using only services provided with a (free, personal) Github account. See [demo setup](/live-demos/mnist-digits-demo-pipeline/).
  Since this can be scheduled to run daily, one could:

    - Run (smaller scale) public data pipelines that process and report on open data.
    - Showcase how to use a library with a publicly running pipeline.

- Improve the tooling to collaborate on notebooks and public open source pipelines.
- Offer example pipeline(s) exploring how MLOps can scale down to minimum "free tier" setups.
- Reproducible science: schedule the analysis for a paper to run eg. every month, potentially with updated dependencies.

## Status

This is work in progress (and even the name `pynb-dag-runner` might change :smile:).

The project is already usable, but not for critical workloads.

[Feedback, ideas and contributions welcome!](/contact)
