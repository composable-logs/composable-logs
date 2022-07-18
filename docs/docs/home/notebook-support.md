- `pynb-dag-runner` supports tasks defined in Jupyter Python notebooks, but can also run pure Python tasks (to verify)
- All tasks in the [demo pipeline](/live-demos/mnist-digits-demo-pipeline/) are notebooks.
- In git, notebooks source files (with code) are assumed to be in the [Jupytext<sup><sup><sub>:material-launch:</sub></sup></sup>](https://jupytext.readthedocs.io/en/latest/) format.
- Jupytext notebook source files:

    - can be version controlled and code reviewed as usual Python text files.
    - support interactive work/editing with Jupyter and VS Code.
    - do not contain any evaluated outputs. Thus any images and data tables (that could contain sensitive information) are not commited to git.
    - can be [Black formatted<sup><sup><sub>:material-launch:</sub></sup></sup>](https://black.readthedocs.io/en/stable/) and type checked with [Mypy<sup><sup><sub>:material-launch:</sub></sup></sup>](http://mypy-lang.org/).

- Notebook tasks are executed using the [Papermill<sup><sup><sub>:material-launch:</sub></sup></sup>](https://papermill.readthedocs.io/en/latest/) library and can be parameterized.
- After a notebook task has run, the evaluated notebook (that include all output cells) is emitted to the OpenTelemetry log. From the UI, evaluated notebooks can be inspected, or downloaded in ipynb or html format.

!!! info
    As motivation for supporting notebooks, there are already Python libraries like [Evidently<sup><sup><sub>:material-launch:</sub></sup></sup>](https://evidentlyai.com/) and [Pandas profiling<sup><sup><sub>:material-launch:</sub></sup></sup>](https://pandas-profiling.ydata.ai) that generate interactive dashboards/UIs inside Jupyter.
    Thus, deploying a public notebook pipeline (as made possible by `pynb-dag-runner`) can be seen as a first step towards a public dashboard.

    A few developments further motivate this direction:

    - JupyterLite make it possible to run Jupyter server in the browser; see [Possible Futures for Jupyter, PyData 7/2022 (YouTube, 20 mins)](https://www.youtube.com/watch?v=4041pEGsW6w). JupyterLite does not yet seem to be supported by existing Jupyter dashboard libraries, but there are initial work in this direction, see eg [link](https://github.com/holoviz/panel/issues/3605).
    - Since Python 3.11 (released 7/2022) includes experimental support for [WebAssembly<sup><sup><sub>:material-launch:</sub></sup></sup>](https://docs.python.org/3.11/whatsnew/3.11.html), it might  in the future become easier to do front-end development directly in Python.
