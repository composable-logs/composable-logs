SHELL := /bin/bash

.PHONY: build run-command shell

# ---

build:
	docker \
	    build \
	    --file Dockerfile.base \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag ci-tools-image \
	    .

run-command:
	echo "-----"
	echo "DOCKER_RUN_ARGS= $(DOCKER_RUN_ARGS)"
	echo "-----"
	echo "COMMAND= $(COMMAND)"
	echo "-----"
	docker run --rm -t \
	    $(DOCKER_RUN_ARGS) \
	    --volume $$(pwd)/../workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    ci-tools-image \
	    "$(COMMAND)"

shell:
	# Open shell in docker image
	make run-command \
	    DOCKER_RUN_ARGS="-i" \
	    COMMAND="/bin/bash"
