SHELL := /bin/bash

docker-build-all:
	(cd docker; make \
	    build-base-env-docker-image \
	    build-cicd-env-docker-image \
	    build-dev-env-docker-image)

### Manually start/stop the dev-docker container (can be used without VS Code)

dev-up:
	docker-compose \
	    -f .devcontainer-docker-compose.yml \
	    up \
	    --remove-orphans \
	    --abort-on-container-exit \
	    dev-environment

dev-down:
	docker-compose \
	    -f .devcontainer-docker-compose.yml \
	    down \
	    --remove-orphans

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

build:
	make COMMAND="(cd pynb_dag_runner; make build)" docker-run-in-cicd

test:
	# Run all tests for library
	make COMMAND="( \
	    cd pynb_dag_runner; \
	    make \
	        test-pytest \
	        test-mypy \
	        test-black \
	)" docker-run-in-cicd
