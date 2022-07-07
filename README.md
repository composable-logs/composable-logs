> *Run ML/Data pipelines on stateless compute infrastructure.*

`pynb-dag-runner` is a Python framework for running ml/data pipelines without a tracking server (or database) to recording task outcomes, or logged ML metrics/artifacts.
Instead key data during a pipeline run is emitted using the [OpenTelemetry standard](https://opentelemetry.io/).

This can be used to run pipelines on limited or no cloud infrastructure, like eg. using Github actions for compute and Github pages for reporting.

**Please see the `pynb-dag-runner` documentation site for details and demo: https://pynb-dag-runner.github.io/pynb-dag-runner**

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
