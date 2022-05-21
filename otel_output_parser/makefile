SHELL := /bin/bash

.PHONY: \
	docker-build \
	docker-shell \
	docker-run-command \
	docker-tmux-watch-all-tests \
	docker-pytest

# --

docker-build:
	(cd docker; make build)

docker-shell:
	(cd docker; make shell)

docker-run-command:
	(cd docker; make run-command COMMAND="$(COMMAND)")

docker-tmux-watch-all-tests:
	# run pytest in watch mode with ability to filter out specific test(s)
	make docker-run-command \
	    ARGS="-i" \
	    COMMAND="( \
	        make tmux-watch-all-tests \
	        WATCH_MODE=1 \
	        PYTEST_FILTER=\"$(PYTEST_FILTER)\" \
	)"

docker-pytest:
	make docker-run-command \
	    COMMAND="( \
	        make pytest \
	            WATCH_MODE=$(WATCH_MODE) \
	            PYTEST_FILTER=\"$(PYTEST_FILTER)\" \
	)"
