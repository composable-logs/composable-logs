SHELL := /bin/bash

.PHONY: pytest mypy black tmux-watch-all-tests

ifdef WATCH_MODE
    $(info ">> Running unit tests in watch mode ...")
    CMD_PREFIX := find . | grep ".py" | entr bash -c
else
    CMD_PREFIX := bash -c
endif

# ---

pytest:
	@$(CMD_PREFIX) "( \
	    date; \
	    pytest \
	        -vvv \
	        -k '$(PYTEST_FILTER)' \
	        --durations=10 \
	        --continue-on-collection-errors tests; \
	)"

mypy:
	# Check static mypy type checks (mypy)
	@$(CMD_PREFIX) "( \
	    date; \
	    mypy \
	        --ignore-missing-imports \
	        static_builder tests common_helpers \
	)"

black:
	# Check that code is Black formatted (black)
	@$(CMD_PREFIX) "( \
	    date; \
	    black \
	        --check \
	        --diff \
	        . \
	)"

tmux-watch-all-tests:
	# desc: Watch all tests in tmux session
	tmuxinator start
