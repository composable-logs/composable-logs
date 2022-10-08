.PHONY: *

SHELL            := /bin/bash
ALL_CODE_FOLDERS := "pynb_dag_runner otel_output_parser tests/test_pynb_dag_runner tests/test_otel_output_parser"

ifdef WATCH_MODE
  $(info ">> Running unit tests in watch mode ...")
  CMD_PREFIX := find . | grep ".py" | entr
else
  CMD_PREFIX :=
endif

test-pytest:
	@echo ">> Running unit tests ..."
	$(CMD_PREFIX) pytest \
	    -k "$(PYTEST_FILTER)" \
	    --durations=10 \
	    --continue-on-collection-errors tests

test-mypy:
	@echo ">> Running mypy type tests ..."
	$(CMD_PREFIX) mypy --ignore-missing-imports --exclude build "$(ALL_CODE_FOLDERS)"

test-black:
	@echo ">> Verify that code is black formatted ..."
	$(CMD_PREFIX) black --check --diff "$(ALL_CODE_FOLDERS)"

build:
	@echo ">> Building library wheel file ..."
	@echo "- PYTHON_PACKAGE_RELEASE_TARGET  : $(PYTHON_PACKAGE_RELEASE_TARGET)"
	@echo "- GITHUB_SHA                     : $(GITHUB_SHA)"
	@echo "- README_FILEPATH                : $(README_FILEPATH)"
	make clean

	python3 setup.py bdist_wheel

clean:
	rm -rf \
	    build \
	    dist \
	    pynb_dag_runner.egg-info


# --- TODO: merge these ---
# ifdef WATCH_MODE
#     $(info ">> Running unit tests in watch mode ...")
#     CMD_PREFIX := find . | grep ".py" | entr bash -c
# else
#     CMD_PREFIX := bash -c
# endif

# # ---

# pytest:
# 	@$(CMD_PREFIX) "( \
# 	    date; \
# 	    pytest \
# 	        -vvv \
# 	        -k '$(PYTEST_FILTER)' \
# 	        --durations=10 \
# 	        --continue-on-collection-errors tests; \
# 	)"

# mypy:
# 	# Check static mypy type checks (mypy)
# 	@$(CMD_PREFIX) "( \
# 	    date; \
# 	    mypy \
# 	        --ignore-missing-imports \
# 	        static_builder tests common_helpers \
# 	)"

# black:
# 	# Check that code is Black formatted (black)
# 	@$(CMD_PREFIX) "( \
# 	    date; \
# 	    black \
# 	        --check \
# 	        --diff \
# 	        . \
# 	)"

# tmux-watch-all-tests:
# 	# desc: Watch all tests in tmux session
# 	tmuxinator start
