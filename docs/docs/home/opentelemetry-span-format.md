# OpenTelemetry span format reference

TODO

!!! info
    Similar to MLFlow, `pynb-dag-runner` can log metrics, images and other artifacts.
    If a pipeline has multiple tasks, `pynb-dag-runner` will emit OpenTelemetry events describing:

    - what tasks ran in the pipeline,
    - runtimes for each task,
    - if tasks fail, information about exceptions, retries and timeouts,
    - task outcomes (success, failure).

    All logs from `pynb-dag-runner` are emitted as [OpenTelemetry span:s](https://opentelemetry.io/docs/concepts/observability-primer/#spans).

    By documenting the log format `pynb-dag-runner` one could add OpenTelemetry support to other pipeline execution backends.
