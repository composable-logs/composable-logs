SHELL := /bin/bash

### Tasks to build Docker images

build-base-env-docker-image:
	docker \
	    build \
	    --file Dockerfile.base \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag pynb-dag-runner-base \
	    .

build-cicd-env-docker-image:
	docker \
	    build \
	    --file Dockerfile.cicd \
	    --tag pynb-dag-runner-cicd \
	    .

JUPYTER_TOKEN:
	# Create random JUPYTER_TOKEN for Jupyter running in Docker
	openssl rand -base64 42 > JUPYTER_TOKEN

build-dev-env-docker-image: JUPYTER_TOKEN
	docker \
	    build \
	    --file Dockerfile.dev \
	    --build-arg JUPYTER_TOKEN=$$(cat JUPYTER_TOKEN) \
	    --tag pynb-dag-runner-dev \
	    .
