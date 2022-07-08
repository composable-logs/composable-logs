**Python library to run ML/data pipelines on stateless compute infrastructure.**

`pynb-dag-runner` is a Python framework for running ml/data pipelines without a tracking server (or database) to record task outcomes, or logged ML metrics/artifacts.
Instead key data during a pipeline run are emitted using the [OpenTelemetry standard](https://opentelemetry.io/).

Since structured logs can be directed to a file, this can be used to run pipelines on limited or no cloud infrastructure.
Eg. one can use Github actions for compute and Github pages for reporting.

**Please see the `pynb-dag-runner` documentation site for details and demo:**
- **https://pynb-dag-runner.github.io/pynb-dag-runner**

Feedback welcome

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
