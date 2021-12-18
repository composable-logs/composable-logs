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

run-in-docker:
	# Run bash command(s) in Docker image DOCKER_IMG (=cicd or dev)

	docker run --rm -t \
	    --network none \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner-$(DOCKER_IMG) \
	    "$(COMMAND)"

docker-run-in-cicd:
	make COMMAND="$(COMMAND)" DOCKER_IMG="cicd" run-in-docker

docker-run-in-dev:
	make COMMAND="$(COMMAND)" DOCKER_IMG="dev" run-in-docker

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

pytest-watch:
	# run pytest in watch mode with ability to filter out specific test(s)
	make COMMAND="( \
	    cd pynb_dag_runner; \
	    make WATCH_MODE=1 PYTEST_FILTER=\"$(PYTEST_FILTER)\" test-pytest \
	)" docker-run-in-dev
