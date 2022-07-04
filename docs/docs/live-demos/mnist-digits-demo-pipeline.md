A goals of `pynb-dag-runner` is to make it easy to deploy data/ml pipelines with
no (or minimal) cloud infrastructure.

Currently there is one live demo pipeline that illustrate this.

# `mnist-digits-demo-pipeline`
This pipeline trains a model for recognizing
hand written digits from a toy MNIST data set included in sklearn library.

Of note:

- [x] The pipeline runs entirely using services provided with a free Github account
- [x] Github integration

- Github: [https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline)
- Experiment tracker: [https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/](https://pynb-dag-runner.github.io/mnist-digits-demo-pipeline/)


### Architecture
``` mermaid
graph LR
  A[Foo] --> B[Bar];
  B --->|back| A
```

### Run locally
...

### Github integration
...
