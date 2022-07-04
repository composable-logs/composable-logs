A goal of `pynb-dag-runner` is to make it easy to deploy data/ml pipelines with
no (or with minimal) cloud infrastructure.

Currently there is one live demo pipeline that illustrates how `pynb-dag-runner` works in practice.

## `mnist-digits-demo-pipeline`

This pipeline trains a model for recognizing hand written digits from a
toy MNIST data set included in sklearn library.

- Github: [https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline)
- Experiment tracker: [https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)


### Main features

- [x] The pipeline is scheduled to run every day and for all pull requests to the repo.
- [x] Outcomes of pipeline runs (both scheduled- and PR- pipeline runs) can be inspected in the
      Experiment tracker (see above, this is hosted as a static website and build using a fork of MLFlow).
- [x] The pipeline runs without any cloud infrastructure, and
      pipeline uses only services provided with a (free personal)
      Github account:
       - Use **Github Actions** for compute resources, and for orchestration.
       - Use **Github Build Artifacts** for storing pipeline run logs.
       - Use **Github Pages** for pipeline/task tracker and ML-experiment tracker
         (hosted as a static site and built from pipeline run logs).
- [x] This setup does not require any databases or tracking servers (eg. for task execution,
      like Airflow) or for experiment tracking (eg. ML tracking server, like an MLFlow backend).

### Architecture
``` mermaid
graph LR
  A[Foo] --> B[Bar];
  B --->|back| A
```

### Run locally

### Github integration
...

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
