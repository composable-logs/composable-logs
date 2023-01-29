The below are (high level) steps for doing local ui development of the static version of mlflow.

## Clone repos

```bash
cd /some-work-dir/

# the pynb-dag-runner repo is needed to
#  - download build artifacts/run logs from demo pipeline
#  - convert these into format that can be compiled into static website
git clone git@github.com:pynb-dag-runner/pynb-dag-runner.git

# The mlflow clone repo contains a fork of the official mlflow project.
# This fork supports building static websites.
git clone git@github.com:pynb-dag-runner/mlflow.git --branch static_mlflow
```

The below steps will also use test pipeline run logs produced from the [mnist-demo-pipeline repo](https://github.com/pynb-dag-runner/mnist-digits-demo-pipeline), but this repo does not need to be cloned locally.

## Download test data
Here we download test data using a PAT (Personal Access Token), see [docs<sup><sup><sub>:material-launch:</sub></sup></sup>](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) if these are not familar. A (safer) approach (that avoids using a PAT) would be to manually download artifacts from the Github UI.

### 1. Generate PAT in Github UI
 - Note that the PAT will have access to everything the logged in user has access to (for selected scopes). So assign minimum priviledges to the PAT:
   - only scope needed is `public repos`.
   - set minimum expirence limit, eg. 1 or 7 days.
   - delete the PAT when no longer needed (step 2b below).

### 2. Start and set up Docker container with dependencies
Eg. start VS Code dev container in the pynb-dag-runner repo's `otel_output_parse` directory.

```bash
pip install -e .
export GITHUB_TOKEN="<see above>"

# verify that generate_static_data cli is installed
generate_static_data --help
```

#### 2a. Download workflow (zip) artifacts into local cache directory
The below will download available build artifacts from past workflow runs into a `./cache` directory.
```bash
rm -rf ./cache    # <-- !!
generate_static_data \
   --github_repository pynb-dag-runner/mnist-digits-demo-pipeline \
   --zip_cache_dir ./cache
```
#### 2b. Delete the token created above in the Github UI

#### 2c. Parse zip file content into form suitable for for static ML Flow website
- this step does not require network/API access and uses only files in the `./cache` directory.
- this will delete any previous content in `./static_output` directory.

```bash
rm -rf static_output    # <-- !!

generate_static_data \
   --zip_cache_dir ./cache \
   --output_www_root_directory ./static_output/www_root
```

### 3. Copy data into ML Flow repo

Copy the outputs into correct directories in the (forked) ML Flow repo:

```bash
# See step above where repos where cloned:
cd /some-work-dir/

rm -rf mlflow/mlflow/server/js/public/pipeline-artifacts    # <-- !!

# TODO: review details for the below commands.
cp -r \
   pynb-dag-runner/otel_output_parser/workspace/static_output/static-data.json \
   mlflow/mlflow/server/js/public/

cp -r \
   pynb-dag-runner/otel_output_parser/workspace/static_output/pipeline-artifacts \
   mlflow/mlflow/server/js/public/

```

### 4. Build static mlflow/run in watch mode

See `makefile` in the `./mlflow/server/js/` directory of the `static_mlflow` branch of the cloned mlflow-repo
