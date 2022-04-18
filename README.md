# `pynb-dag-runner` Python library

> **py_dag_runner** is a light-weight open source Python library for running pipelines of Python functions and/or Jupter notebooks.

Main features:

### Tasks are executed in parallel using the Ray framework
 - With a DAG one can define in which order tasks should run.
 - Tasks run in parallel using the Ray framework (subject to DAG constraints).

### Reporting

After a pipeline has run, `py_dag_runner` stores outputs from all tasks in the pipeline to a directory structure. This includes:

- Evaluated notebooks in both `.ipynb` and `.html` formats.
- Parameters used to trigger tasks.
- Timing and other outcomes (eg., did a task fail, succeed, or timeout).

An implication of storing output as files is that `py_dag_runner` is independent of any external service, database, or infrastructure. Eg., if `py_dag_runner` is used to run a pipeline in a CI-setup all outputs can be saved as build-artefacts.

A limitation of this approach is that there is no real-time monitoring. For interactive viewing, output files can be inspected with a file viewer. Or, alternatively, artefacts can be uploaded to ML Flow for off-line inspection.

###  Demo pipeline, see [mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline) repo

![task-dependencies.png](./assets/task-dependencies.png)

## Notes

Jupyter notebooks are assumed to be in Jupytext format. That means:

1. Notebooks are stored (in git) as ordinary Python text files that can be version controlled and reviewed in pull requests.
2. Unlike ipynb files, Jupytext notebooks can not contain data cells (with images or tables).
3. Notebooks are run with the Papermill library [link](https://papermill.readthedocs.io/en/latest/), and notebooks can therefore also depend on parameters.

With the [Jupytext format](https://jupytext.readthedocs.io/en/latest/formats.html), notebooks can be linted (using black) and type checked (using mypy), see demo pipeline linked above for an example of this.

The [Ray framework](https://www.ray.io/) supports execution on a Ray cluster of multiple nodes, but currently `py_dag_runner` only supports single node setups.

----

## Development setup

Development environments are dockerized, and makefile:s should be available for most common tasks. See:

- [docs/LOCAL_UI_DEV.md](docs/LOCAL_UI_DEV.md): local development for developing mlflow frontend with static data

- [docs/LOCAL_DEV.md](docs/LOCAL_DEV.md): local development of main `pynb_dag_runner` Python library

The Github actions in the `pynb-dag-runner` and `mnist-digits-demo-pipeline` repos can also be useful since they show how various tests (unit, static code analysis) and pipelines are run in docker.

Instructions assume linux-based setup (eg. ubuntu), but should also work on macs with some changes (or at least on non-M1 ones).

## Contact

Please note that this is experimental and 🚧🚧🚧🚧.

A motivation for this work is to make it easier to set up and work together (on pipelines). If you would like to discuss an idea or question, please raise an [issue](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline/issues) or contact me via email.

## License

(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
