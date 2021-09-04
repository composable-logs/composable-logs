SHELL := /bin/bash

### Tasks to build Docker images

docker-build-base-env:
	# setup with Python dependencies required downstreams, "prod"
	docker \
	    build \
	    --file ./docker/Dockerfile.base \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag pynb-dag-runner-base \
	    ./docker

docker-build-cicd-env:
	# setup for cicd (test + build)
	docker \
	    build \
	    --file ./docker/Dockerfile.cicd \
	    --tag pynb-dag-runner-cicd \
	    ./docker

JUPYTER_TOKEN:
	# Create random JUPYTER_TOKEN for Jupyter running in Docker
	openssl rand -base64 42 > JUPYTER_TOKEN

docker-build-dev-env: JUPYTER_TOKEN
	# setup for interactive Jupyter development
	docker \
	    build \
	    --file ./docker/Dockerfile.dev \
	    --build-arg JUPYTER_TOKEN=$$(cat JUPYTER_TOKEN) \
	    --tag pynb-dag-runner-dev \
	    ./docker

docker-build-all:
	make \
	    docker-build-base-env \
	    docker-build-cicd-env \
	    docker-build-dev-env

### Manually start/stop the dev-docker container (can be used without VS Code)

dev-up:
	docker-compose up --remove-orphans --abort-on-container-exit dev-environment

dev-down:
	docker-compose down --remove-orphans

### Define tasks run inside Docker

docker-run-in-cicd:
	# Run bash command(s) in (non-dev) Docker container.

	docker run --rm \
	    --network none \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner-cicd \
	    "$(COMMAND)"

clean:
	make COMMAND="(cd pynb_dag_runner; make clean)" docker-run-in-cicd

test-library:
	# Run all tests for library and ensure we can build the library wheel file

	make COMMAND="(\
	    cd pynb_dag_runner; \
	    make \
	        test-pytest \
	        test-mypy \
	        test-black \
	        build \
	)" docker-run-in-cicd
