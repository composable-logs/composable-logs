<div align="center">

  <!-- need an empty line above: https://stackoverflow.com/a/70293384 -->
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/actions/workflows/ci.yml">![automated tests](https://github.com/pynb-dag-runner/pynb-dag-runner/actions/workflows/ci.yml/badge.svg)</a>
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/blob/development/LICENSE.md">![license=mit](https://img.shields.io/badge/license-MIT-blue)</a>
  <a href="https://github.com/pynb-dag-runner/pynb-dag-runner/issues/new">![Ideas and feedback=welcome](https://img.shields.io/badge/Ideas%20%26%20feedback-welcome-green)</a>

</div>

---

**`pynb-dag-runner` is a Python library that can run ML/data pipelines on stateless compute infrastructure (that may be ephemeral or serverless).**

This means that `pynb-dag-runner` does not need a tracking server (or database) to record task outcomes/logged ML metrics/artifacts.
Instead the key data during a pipeline run are emitted using the [OpenTelemetry standard](https://opentelemetry.io/).

Since structured logs can be directed to a file (as one option), this can be used to run pipelines on limited or no cloud infrastructure.
Eg. one can use Github actions for compute and Github pages for reporting.

**Please see the documentation for details and demo:**
- **https://pynb-dag-runner.github.io/pynb-dag-runner**

Any feedback/ideas welcome!

## License
(c) Matias Dahl 2021-2022, MIT, see [LICENSE.md](./LICENSE.md).
