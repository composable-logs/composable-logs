SHELL              := /bin/bash
DOCKER_IMAGE_NAME  := pynb-dag-runner-docs

.PHONY: *

docker-build:
	docker build \
	    --file Dockerfile \
	    --build-arg HOST_UID=$(shell id -u) \
	    --build-arg HOST_GID="$(shell id -g)" \
	    --tag ${DOCKER_IMAGE_NAME} \
	    .

docker-mkdocs-serve:
	docker run \
	    --tty \
	    --rm \
	    -p 4200:4200 \
	    -v $(shell pwd):/home/host_user/docs:ro \
	    --workdir /home/host_user/docs \
	    ${DOCKER_IMAGE_NAME} \
	    "mkdocs serve --dev-addr 0.0.0.0:4200"

docker-mkdocs-build:
	docker run \
	    --tty \
	    --rm \
	    -v $(shell pwd):/home/host_user/docs \
	    --workdir /home/host_user/docs \
	    ${DOCKER_IMAGE_NAME} \
	    "mkdocs build \
	        --clean \
	        --strict \
	        --site-dir /home/host_user/docs/www-root \
		"

clean:
	rm -rf www-root
