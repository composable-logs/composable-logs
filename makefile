.PHONY: *
SHELL := /bin/bash

# --- Helper tasks ---

env_%:
	@# Check that a variable is defined, see stackoverflow.com/a/7367903
	@if [[ -z "$($*)" ]]; then exit 1; fi

# --- docker related tasks ---

build-docker-images:
	(cd docker; make \
	    build-cd-env-docker-image \
	    build-ci-env-docker-image \
	    build-dev-env-docker-image)

# --- recipes to run commands inside Docker images ---

run-in-docker: | env_COMMAND env_DOCKER_IMG
	@# Run bash command(s) in Docker image DOCKER_IMG (=cicd or dev)
	docker run --rm --tty \
	    ${DOCKER_ARGS} \
	    --volume $(shell pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner-$(DOCKER_IMG) \
	    "${COMMAND}"

run-command[in-cd-docker]: | env_COMMAND
	$(MAKE) run-in-docker \
	    DOCKER_ARGS="${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
		DOCKER_IMG="base"

run-command[in-ci-docker]: | env_COMMAND
	@# Note: ci jobs run without network
	@# DOCKER_ARGS optional
	$(MAKE) run-in-docker \
	    DOCKER_ARGS="--network none ${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
	    DOCKER_IMG="cicd"

in-dev-docker/run-command: | env_COMMAND
	@# Note: ci jobs run without network
	@# DOCKER_ARGS optional
	$(MAKE) run-in-docker \
	    DOCKER_ARGS="--network none ${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
	    DOCKER_IMG="dev"

# --- define dockerized recipes for testing and building pynb-dag-runner package---

in-ci-docker/clean:
	cd docker; \
	$(MAKE) in-ci-docker/run-command \
	    COMMAND="(cd pynb_dag_runner; make clean)"

in-ci-docker/build: | env_GITHUB_SHA env_PYTHON_PACKAGE_RELEASE_TARGET env_LAST_COMMIT_UNIX_EPOCH
	cd docker; \
	$(MAKE) in-ci-docker/run-command \
	    DOCKER_ARGS=" \
	        -e GITHUB_SHA \
	        -e PYTHON_PACKAGE_RELEASE_TARGET \
	        -e LAST_COMMIT_UNIX_EPOCH \
	        --volume $(shell pwd):/repo-root:ro \
	    " \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make build \
	            README_FILEPATH=/repo-root/README.md; \
		)"

in-dev-docker/watch-pytest:
	@# run pytest in watch mode. Filter test names with PYTEST_FILTER argument
	cd docker; \
	$(MAKE) in-dev-docker/run-command \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make watch-test-pytest \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"

in-dev-docker/tmux-watch-all-tests:
	@# run all tests in tmux/watch mode. Filter unit tests with PYTEST_FILTER argument
	cd docker; \
	$(MAKE) in-dev-docker/run-command \
	    DOCKER_ARGS="-i" \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make tmux-watch-all-tests \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"
