# Next steps (as of 7/2022)

## Improve pipeline execution speed

Developer experience is currently limited by slow pipeline execution.
For example, in the `mnist-demo-pipeline` demo pipeline, the actual pipeline
tasks executes in ~2 minutes,
but entire pipeline (with website update) takes total of ~50 minutes with all overhead
(when run on Github actions, see
[Github action logs<sup><sup><sub>:material-launch:</sub></sup></sup>](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline/actions)).

Subtasks:

- [ ] Avoid recompiling MLFlow UI for each deployment. That is, instead of
      embedding all metadata shown in the UI as a static variable, load it from a
      static JSON file.
- [ ] Faster parsing of OpenTelemetry logs (using a suitable tree data structure).
- [ ] Do not convert DAGs and Gantt diagrams into png:s. Rather, render the
      Mermaid diagrams in the UI.
- [ ] Offer `pynb-dag-runner` as pip install package.

A goal would to execute pipeline + website update in ~5-10 minutes?

---

## Improve documentation

Eg this website.
