SHELL := /bin/bash

DOCKER_IMAGE_NAME       := composable-logs
BUILT_DOCKER_LOCK_FILE  := ./.built-docker-images/${DOCKER_IMAGE_NAME}

# --- Helper tasks ---

env_%:
	@# Check that a variable is defined, see stackoverflow.com/a/7367903
	@if [[ -z "${$*}" ]]; then exit 1; fi


# --- Tasks to build Docker images ---

${BUILT_DOCKER_LOCK_FILE}-base: Dockerfile.base \
                                makefile
	docker build \
	    --no-cache \
	    --file Dockerfile.base \
	    --build-arg HOST_UID=$(shell id -u) \
	    --build-arg HOST_GID="$(shell id -g)" \
	    --tag ${DOCKER_IMAGE_NAME}-base \
	    .
	mkdir -p $(shell dirname $@)
	touch $@

build-docker-base-image: ${BUILT_DOCKER_LOCK_FILE}-base
	@# For now, the cd-image is used to deploy the Python package, so
	@# provide separate task to build only this image

${BUILT_DOCKER_LOCK_FILE}-cicd: ${BUILT_DOCKER_LOCK_FILE}-base \
                                Dockerfile.cicd \
                                requirements.txt \
                                makefile
	docker build \
	    --no-cache \
	    --file Dockerfile.cicd \
	    --tag ${DOCKER_IMAGE_NAME}-cicd \
	    .
	mkdir -p $(shell dirname $@)
	touch $@

JUPYTER_TOKEN:
	# Create random JUPYTER_TOKEN for Jupyter running in Docker
	openssl rand -base64 42 > JUPYTER_TOKEN

${BUILT_DOCKER_LOCK_FILE}-dev: JUPYTER_TOKEN \
                               ${BUILT_DOCKER_LOCK_FILE}-cicd \
                               Dockerfile.dev \
                               makefile
	docker build \
	    --no-cache \
	    --file Dockerfile.dev \
	    --build-arg JUPYTER_TOKEN=$(shell cat JUPYTER_TOKEN) \
	    --tag ${DOCKER_IMAGE_NAME}-dev \
	    .
	mkdir -p $(shell dirname $@)
	touch $@

build-docker-images: ${BUILT_DOCKER_LOCK_FILE}-dev


# --- run command in a docker container ---

run-in-docker: | env_COMMAND env_DOCKER_IMG
	@# Run bash command(s) in Docker image DOCKER_IMG (=base, cicd or dev)
	docker run --rm --tty \
	    ${DOCKER_ARGS} \
	    --volume $(shell pwd)/../workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    composable-logs-${DOCKER_IMG} \
	    "${COMMAND}"

in-base-docker/run-command: | env_COMMAND
	${MAKE} run-in-docker \
	    DOCKER_ARGS="${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
		DOCKER_IMG="base"

in-dev-docker/run-command: | env_COMMAND
	@# Note:
	@#  - dev jobs run without network access
	@#  - additional DOCKER_ARGS optional
	${MAKE} run-in-docker \
	    DOCKER_ARGS="--network none ${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
	    DOCKER_IMG="dev"

in-ci-docker/run-command: | env_COMMAND
	@# Note: ci jobs run without network
	@# DOCKER_ARGS optional
	${MAKE} run-in-docker \
	    DOCKER_ARGS="--network none ${DOCKER_ARGS}" \
	    COMMAND="${COMMAND}" \
	    DOCKER_IMG="cicd"
