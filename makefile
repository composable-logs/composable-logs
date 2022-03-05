SHELL := /bin/bash

docker-build:
	docker build \
	    --file docker/Dockerfile.base \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag ci-tools-image \
	    ./docker

### Define tasks run inside Docker

run-in-docker:
	# Run bash command(s) in Docker image DOCKER_IMG (=cicd or dev)

	docker run --rm -t \
	    --network none \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    ci-tools-image \
	    "$(COMMAND)"

test:
	# Run all tests for library
	make COMMAND="( \
	    cd .; \
	    make \
	        test-pytest \
	        test-mypy \
	        test-black \
	)" docker-run-in-cicd
