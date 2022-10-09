.PHONY: *

SHELL            := /bin/bash
ALL_CODE_FOLDERS := "pynb_dag_runner otel_output_parser tests/test_pynb_dag_runner tests/test_otel_output_parser"

# --- test related tasks ---

test-pytest:
	@echo "$$(date): Running unit tests ..."
	@pytest \
	    -k "$(PYTEST_FILTER)" \
	    --durations=10 \
	    --continue-on-collection-errors \
	    tests

test-mypy:
	@echo "$$(date): Running mypy type tests ..."
	@mypy \
	    --ignore-missing-imports \
	    --exclude build \
	    "$(ALL_CODE_FOLDERS)"

test-black:
	@echo "$$(date): Verify that code is black formatted ..."
	@black \
	    --check \
	    --diff \
	    "$(ALL_CODE_FOLDERS)"

# Define
#    watch-test-pytest
#    watch-test-mypy
#    watch-test-black
# to run above tasks in watch mode.
#
# In watch mode:
#  - test task is run when a Python file is saved.
#  - This only watches files that existed when the test task started.
#
watch-%:
	@find . | grep ".py" | entr bash -c "$(MAKE) --no-print-directory $*"

# Note:
#  - both watch-all and watch-test-pytest accept PYTEST_FILTER parameter.
watch-all:
	@tmuxinator start

# --- build related tasks ---

build:
	@echo ">> Building library wheel file ..."
	@echo "- PYTHON_PACKAGE_RELEASE_TARGET  : $(PYTHON_PACKAGE_RELEASE_TARGET)"
	@echo "- GITHUB_SHA                     : $(GITHUB_SHA)"
	@echo "- README_FILEPATH                : $(README_FILEPATH)"
	$(MAKE) clean

	python3 setup.py bdist_wheel

clean:
	rm -rf \
	    build \
	    dist \
	    pynb_dag_runner.egg-info
