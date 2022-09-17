.PHONY: *
SHELL := /bin/bash

# --- Helper tasks ---

env_%:
	@# Check that a variable is defined, see stackoverflow.com/a/7367903
	@if [[ -z "${$*}" ]]; then exit 1; fi

# --- Main tasks ---

docker-build-all:
	(cd docker; make \
	    build-cd-env-docker-image \
	    build-ci-env-docker-image \
	    build-dev-env-docker-image)

### Manually start/stop the dev-docker container (can be used without VS Code)

dev-up:
	docker-compose \
	    -f .devcontainer-docker-compose.yml \
	    up \
	    --remove-orphans \
	    --abort-on-container-exit \
	    dev-environment

dev-down:
	docker-compose \
	    -f .devcontainer-docker-compose.yml \
	    down \
	    --remove-orphans

### Define tasks run inside Docker

run-in-docker: | env_COMMAND env_DOCKER_IMG
	@# Run bash command(s) in Docker image DOCKER_IMG (=cicd or dev)
	docker run --rm -t \
	    ${DOCKER_ARGS} \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner-${DOCKER_IMG} \
	    "${COMMAND}"

docker-run-in-cicd: | env_COMMAND
	# ---- deprecated; move to run[in-cicd-docker] ----
	make COMMAND="${COMMAND}" DOCKER_IMG="cicd" run-in-docker

run-command[in-cd-docker]: | env_COMMAND
	make run-in-docker \
	    DOCKER_ARGS="${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
		DOCKER_IMG="base"

run-command[in-ci-docker]: | env_COMMAND
	@# Note: ci jobs run without network
	@# DOCKER_ARGS optional
	make run-in-docker \
	    DOCKER_ARGS="--network none ${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
		DOCKER_IMG="cicd"

docker-run-in-dev: | env_COMMAND
	make COMMAND="${COMMAND}" DOCKER_IMG="dev" run-in-docker

clean[in-ci-docker]:
	make COMMAND="(cd pynb_dag_runner; make clean)" run-command[in-ci-docker]

build[in-ci-docker]: | env_GITHUB_SHA env_PYTHON_PACKAGE_RELEASE_TARGET env_LAST_COMMIT_UNIX_EPOCH
	make run-command[in-ci-docker] \
	    DOCKER_ARGS=" \
	        -e GITHUB_SHA \
	        -e PYTHON_PACKAGE_RELEASE_TARGET \
	        -e LAST_COMMIT_UNIX_EPOCH \
	        --volume $$(pwd):/repo-root:ro \
	    " \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make build \
	            README_FILEPATH=/repo-root/README.md; \
		)"

test[in-ci-docker]:
	@# Run all tests for library
	make run-command[in-ci-docker] \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make \
	            test-pytest \
	            test-mypy \
	            test-black \
	    )"

pytest-watch:
	@# run pytest in watch mode with ability to filter out specific test(s)
	make docker-run-in-dev \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make WATCH_MODE=1 PYTEST_FILTER=\"$(PYTEST_FILTER)\" test-pytest \
	    )"
