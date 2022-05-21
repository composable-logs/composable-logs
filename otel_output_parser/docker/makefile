SHELL := /bin/bash

.PHONY: build run-command shell

# --

build:
	docker \
	    build \
	    --file Dockerfile.base \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag ci-tools-image \
	    .

run-command:
	docker run --rm -t \
	    $(ARGS) \
	    --network none \
	    --volume $$(pwd)/../workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    ci-tools-image \
	    "$(COMMAND)"

shell:
	# Open shell in docker image
	make run-command \
	    ARGS="-i" \
	    COMMAND="/bin/bash"
