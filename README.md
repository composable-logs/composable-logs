Run pipelines on stateless compute infrastructure.

`pynb-dag-runner` is a Python framework for running ml/data pipelines that does not require a tracking server (or database) to recording task outcomes, or logged ML metrics/artifacts.
Instead key data during a pipeline run is emitted using the [OpenTelemetry standard](https://opentelemetry.io/).

This can be used to run pipelines on limited or no cloud infrastructure, like Github actions for compute.

**Please see the `pynb-dag-runner` documentation site for more details: https://pynb-dag-runner.github.io/pynb-dag-runner**

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
