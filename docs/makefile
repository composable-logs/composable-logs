SHELL := /bin/bash

.PHONY: docker-build
docker-build:
	docker build \
	    --file Dockerfile \
	    --build-arg HOST_UID=$$(id -u) \
	    --build-arg HOST_GID="$$(id -g)" \
	    --tag pynb-dag-runner-docs \
	    .

.PHONY: docker-mkdocs-serve
docker-mkdocs-serve:
	docker run \
	    -t --rm \
		-p 4200:4200 \
		-v $$(pwd):/home/host_user/docs \
		--workdir /home/host_user/docs \
		pynb-dag-runner-docs \
		"mkdocs serve --dev-addr 0.0.0.0:4200"

.PHONY: docker-mkdocs-build
docker-mkdocs-build:
	docker run \
	    -t --rm \
	    -v $$(pwd):/home/host_user/docs \
	    --workdir /home/host_user/docs \
	    pynb-dag-runner-docs "\
	        mkdocs build \
	            --clean \
	            --strict \
	            --site-dir /home/host_user/docs/site_dir \
		"

.PHONY: site_dir
clean:
	rm -rf site_dir
