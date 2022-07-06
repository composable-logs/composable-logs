# OpenTelemetry schema

!!! info
    `pynb-dag-runner` uses the (open) OpenTelemetry standard for logging everything.
    In more detail, log messages are emitted as [OpenTelemetry span:s](https://opentelemetry.io/docs/concepts/observability-primer/#spans).

    Similar to MLFlow, `pynb-dag-runner` can log metrics, images and other artifacts.
    In addition, `pynb-dag-runner` logs run data like:

    - what tasks ran in the pipeline,
    - runtimes for each task,
    - task outcomes (success, failure),
    - if tasks fail, information about exceptions, retries and timeouts.

    By documenting the log format `pynb-dag-runner` one could add OpenTelemetry support to other pipeline execution backends.

    TODO
