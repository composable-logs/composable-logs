SHELL := /bin/bash

docker-build-all:
	(cd docker; make \
	    build-cd-env-docker-image \
	    build-ci-env-docker-image \
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
	@# Run bash command(s) in Docker image DOCKER_IMG (=cicd or dev)
	docker run --rm -t \
	    $(DOCKER_ARGS) \
	    --volume $$(pwd)/workspace:/home/host_user/workspace \
	    --workdir /home/host_user/workspace/ \
	    pynb-dag-runner-$(DOCKER_IMG) \
	    "$(COMMAND)"

docker-run-in-cicd:
	# ---- deprecated; move to run[in-cicd-docker] ----
	make COMMAND="$(COMMAND)" DOCKER_IMG="cicd" run-in-docker

run-command[in-cd-docker]:
	make run-in-docker \
	    DOCKER_ARGS="$(DOCKER_ARGS)" \
	    COMMAND="$(COMMAND)" \
		DOCKER_IMG="base"

run-command[in-ci-docker]:
	@# Note: ci jobs run without network
	make run-in-docker \
	    DOCKER_ARGS="--network none $(DOCKER_ARGS)" \
	    COMMAND="$(COMMAND)" \
		DOCKER_IMG="cicd"

docker-run-in-dev:
	make COMMAND="$(COMMAND)" DOCKER_IMG="dev" run-in-docker

clean:
	make COMMAND="(cd pynb_dag_runner; make clean)" run-command[in-ci-docker]

build:
	make COMMAND="(cd pynb_dag_runner; make build)" run-command[in-ci-docker]

test:
	# Run all tests for library
	make COMMAND="( \
	    cd pynb_dag_runner; \
	    make \
	        test-pytest \
	        test-mypy \
	        test-black \
	)" run-command[in-ci-docker]

pytest-watch:
	# run pytest in watch mode with ability to filter out specific test(s)
	make COMMAND="( \
	    cd pynb_dag_runner; \
	    make WATCH_MODE=1 PYTEST_FILTER=\"$(PYTEST_FILTER)\" test-pytest \
	)" docker-run-in-dev
