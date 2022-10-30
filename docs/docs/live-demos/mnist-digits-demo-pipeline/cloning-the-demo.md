The demo pipeline [mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline) uses only services associated with a free personal Github account, but no other cloud infrastructure.

This means that anyone can create their own demo pipeline (with scheduled pipeline runs and ci/cd automation) by just cloning the above Github repository.

However, since Github disables actions when a repo is cloned (to save compute), some setup is required.
The below describes how to enable scheduled runs and ci/cd automation after the demo repo has been cloned.

!!! info
    In the interest of saving compute and energy, please do not leave scheduled pipelines running that are not needed.

    Please also note that depending on usage Github Actions may not be free.

#### Steps to clone the demo pipeline

##### 1. Fork the demo pipeline repo [mnist-digits-demo-pipeline](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline):

- Select "Copy the `main` branch only"

##### 2. Open the "Action" tab in your cloned repo:

- When asked, enable actions for this repo.
- Go through all workflows (in the left menu) and enable any workflow that are disabled.
- Manually "Run workflow" for the workflow "Run automated tests, pipeline and deploy results to static reporting site" workflow (after enabling it)

##### 3. After the pipeline is finished, go to "Pages" under "Settings":

Select

- Source: "Deploy from a branch"
- Branch: `gh-pages` and `/ (root)`; press "Save"

After the site has been deployed, a link to the static website should appear in this setup screen.

Note: due a bug in (in the demo pipeline setup), the latest run might not show up in the UI. So to see data in the UI, you might need to trigger the workflow in Step 2 again.
