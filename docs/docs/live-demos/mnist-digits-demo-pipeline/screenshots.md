---
hide:
  #- navigation
  - toc
---

# Demo pipeline reporting screenshots

Below are screeshots from the `mnist-digits-demo-pipeline` reporting website.

- **[https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)**

This is a static website (hosted as a Github Page) and it is updated whenever the demo pipeline has executed.

The demo pipeline is configured to run daily and for all PRs to the main repo.

Thus, this demoes how a sequence of notebooks could be scheduled to run daily with a reporting website.

## Main page: List all pipeline runs

<!-- Image link: https://github.com/pynb-dag-runner/pynb-dag-runner/pull/131 -->
<figure markdown>
  [![list-pipeline-runs.png](https://user-images.githubusercontent.com/10188263/179540915-a4f4e8f7-ff00-4386-8ee7-637df2e634e4.png){ width="800"}](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)
</figure>

## View a particular pipeline run

Selecting a pipeline run shows all tasks that run as part of the pipeline:

<!-- Image link: https://github.com/pynb-dag-runner/pynb-dag-runner/pull/131 -->
<figure markdown>
  [![list-pipeline-runs.png](https://user-images.githubusercontent.com/10188263/179541330-2b416ef5-00ad-406d-904c-d482f23ac899.png){ width="800"}](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)
</figure>

## View a task run
In this demo pipeline, each task in the DAG is defined by a Jupyter notebook.

Inspecting one executed task shows:

- any parameters passed into the task;
- any images, files or metrics that have been logged during the task execution;
- the evaluated notebook for the task as below (unless task was pure Python function).

<!-- Image link: https://github.com/pynb-dag-runner/pynb-dag-runner/pull/131 -->
<figure markdown>
  [![list-pipeline-runs.png](https://user-images.githubusercontent.com/10188263/179541673-178acce8-73c6-4d2e-b55d-1c09bc92b0f2.png){ width="800"}](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)
</figure>

---

Technically, the site is built with a modified version of MLFlow that can be hosted as a static website.
Currently, data shown in the web UI (eg. logged metrics) are compiled into the front end, and larger files (eg. images) are served as static files using Github Pages.

With the current setup, pipeline runs are persisted on Github for 20 days.
