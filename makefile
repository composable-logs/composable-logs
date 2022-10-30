.PHONY: *
SHELL := /bin/bash

# --- Helper tasks ---

env_%:
	@# Check that a variable is defined, see stackoverflow.com/a/7367903
	@if [[ -z "$($*)" ]]; then exit 1; fi


# --- docker related tasks ---

build-docker-images:
	@(cd docker; ${MAKE} build-docker-images)


# --- define main dockerized tasks for testing and building pynb-dag-runner Python package---

clean:
	@(cd workspace/pynb_dag_runner; ${MAKE} clean)

in-ci-docker/build: | env_GITHUB_SHA env_PYTHON_PACKAGE_RELEASE_TARGET env_LAST_COMMIT_UNIX_EPOCH
	# TODO: allow local run
	cd docker; \
	${MAKE} in-ci-docker/run-command \
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
	${MAKE} in-dev-docker/run-command \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make watch-test-pytest \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"

in-dev-docker/tmux-watch-all-tests:
	@# run all tests in tmux/watch mode. Filter unit tests with PYTEST_FILTER argument
	cd docker; \
	${MAKE} in-dev-docker/run-command \
	    DOCKER_ARGS="-i" \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make tmux-watch-all-tests \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"
