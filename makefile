.PHONY: *
SHELL     := /bin/bash
MAKEFLAGS += --no-print-directory

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

in-ci-docker/build: | env_GITHUB_SHA \
                      env_PYTHON_PACKAGE_RELEASE_TARGET \
					  env_LAST_COMMIT_UNIX_EPOCH
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
	@# run pytest in watch mode
	@#
	@#  - Unit test names can be filtered with optional PYTEST_FILTER argument
	@#    (for faster feedback during development)
	@#  - Only files that exist then the task is started are watched.
	@#
	cd docker; \
	${MAKE} in-dev-docker/run-command \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make watch-test-pytest \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"

in-dev-docker/tmux-watch-all-tests:
	@# run all tests (unit, mypy, black) in watch mode
	@#
	@#  - This can be run in the terminal (eg without VS Code) and uses tmux to
	@#    split terminal into three panes for different tests.
	@#  - Same comments as for `in-dev-docker/watch-pytest` apply here.
	@#
	cd docker; \
	${MAKE} in-dev-docker/run-command \
	    DOCKER_ARGS="-i" \
	    COMMAND="( \
	        cd pynb_dag_runner; \
	        make tmux-watch-all-tests \
	            PYTEST_FILTER=\"${PYTEST_FILTER}\" \
	    )"

in-ci-docker/build-local: | clean
	@# Build wheel file for local use (in in demo pipeline dev)
	@${MAKE} in-ci-docker/build \
	    GITHUB_SHA="$$(git rev-parse HEAD)" \
	    PYTHON_PACKAGE_RELEASE_TARGET="snapshot-release" \
	    LAST_COMMIT_UNIX_EPOCH="100000"

	@echo "-------- wheel files ---------"
	@find . | grep "whl"
	@echo "------------------------------"
