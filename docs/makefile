SHELL := /bin/bash

.PHONY: docker-build
docker-build:
	docker \
	    build \
	    --file Dockerfile \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag pynb-dag-runner-docs \
	    .
