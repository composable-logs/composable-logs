## Notes about dependencies

### Ray
- The [Ray framework](https://www.ray.io/) supports execution on a Ray cluster of multiple nodes, but currently `py_dag_runner` only supports single node setups.
- TBD

### OpenTelemetry
- TBD

### Jupytext
Jupyter notebooks are assumed to be in Jupytext format. That means:

- Notebooks are stored (in git) as ordinary Python text files that can be version controlled and reviewed in pull requests.
- Unlike ipynb files, Jupytext notebooks can not contain data cells (with images or tables).

With the [Jupytext format](https://jupytext.readthedocs.io/en/latest/formats.html), notebooks can be linted (using black) and type checked (using mypy), see demo pipeline linked above for an example of this.

### Papermill
- Jupyter notebooks are run with the Papermill library [link](https://papermill.readthedocs.io/en/latest/). Notebooks can therefore depend on parameters.
- If Papermill would support OpenTelemetry logging, one could potentially also in real time follow how cells are executed in a notebook.
