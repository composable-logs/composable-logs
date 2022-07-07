# Projects with similarity to `pynb-dag-runner`

ML pipeline deployment tools (part of MLOps) are now actively developed.

Additions and corrections are welcome.

## Open source projects

### MLFlow 2.0
- [Blog annoucement 6/2022](https://databricks.com/blog/2022/06/29/introducing-mlflow-pipelines-with-mlflow-2-0.html)
- Includes workflow (pipeline)-level reporting (similar as done in `pynb-dag-runner`)

### ZenML
- Also includes support for running pipelines on Github actions compute infra, [6/2022 blog post](https://blog.zenml.io/github-actions-orchestrator/). However, the setup seems to require cloud storage.
- [https://zenml.io](https://zenml.io)

### SAME (**S**elf **A**ssembling **M**achine Learning **E**nvironments)

- Open source project to improve workflow for developing and deploying Jupyter notebooks. The SAME framework supports:

    - local notebook development with Jupyter notebooks;
    - easy deployment to production on various execution backends (like eg. Kubeflow, Azure functions, etc);
    - declarative configuration.

- [Project presentation at Reinforce AI Conference 5/2022, Youtube (37 mins)](https://www.youtube.com/watch?v=akAH1btyxnI)
- [https://sameproject.ml/](https://sameproject.ml/)

## Other

### Gitlab's MLOps initiative
- Gitlab is currently (7/2022) improving support for ML workflows.

    - This includes (plans for) better support for notebooks and per-repo MLFlow support.
    - Available as open source?

- [GitLab MLFlow Integration - What Why and How, Youtube (8 mins)](https://www.youtube.com/watch?v=V4hos3VFeC4)
- [https://about.gitlab.com/handbook/engineering/incubation/mlops/](https://about.gitlab.com/handbook/engineering/incubation/mlops/)
