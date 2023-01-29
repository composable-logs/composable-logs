# OpenTelemetry schema

!!! info
    `composable-logs` uses the (open) OpenTelemetry standard for logging everything.
    In more detail, log messages are emitted as [OpenTelemetry span:s<sup><sup><sub>:material-launch:</sub></sup></sup>](https://opentelemetry.io/docs/concepts/observability-primer/#spans).

    Similar to MLFlow, `composable-logs` can log metrics, images and other artifacts.
    In addition, `composable-logs` logs run data like:

    - what tasks ran in the workflow,
    - runtimes for each task,
    - task outcomes (success, failure),
    - if tasks fail, information about exceptions, retries and timeouts.

    By documenting the log format `composable-logs` one could add OpenTelemetry support to other workflow execution backends.

    TODO
