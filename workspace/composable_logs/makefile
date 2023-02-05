.PHONY: *

SHELL            := /bin/bash
ALL_CODE_FOLDERS := "composable_logs otel_output_parser tests/test_composable_logs tests/test_otel_output_parser"


# --- test related tasks ---

test-pytest:
	@echo "$(shell date): Running pytest unit tests ..."
	@pytest \
	    -k "${PYTEST_FILTER}" \
	    --durations=10 \
	    --continue-on-collection-errors \
	    tests

test-mypy:
	@echo "$(shell date): Running mypy static type checking ..."
	@mypy \
	    --ignore-missing-imports \
	    --exclude build \
	    --check-untyped-defs \
	    "${ALL_CODE_FOLDERS}"

test-black:
	@echo "$(shell date): Verify that code is black formatted ..."
	@black \
	    --check \
	    --diff \
	    "${ALL_CODE_FOLDERS}"


# --- allow us to run tests in watch mode ---

# Define recipes:
#    watch-test-pytest
#    watch-test-mypy
#    watch-test-black
# that run the above test-recipes in watch mode.
#
# In watch mode:
#  - test task is run when a Python file is saved.
#  - This only watches files that existed when the test task started.
#
watch-%:
	@find . \
	    | grep ".py" \
	    | entr \
	    bash -c "${MAKE} --no-print-directory $*"

tmux-watch-all-tests:
	@echo ">>> Running all tests in watch mode. "
	@echo " - Exit with <Ctrl-B> + 'X' + 'y'"
	@echo " - pytest tests filtered with PYTEST_FILTER = ${PYTEST_FILTER}"
	@tmuxinator start


# --- build related tasks ---

clean:
	@echo ">>> Deleting build directories ..."
	rm -rf \
	    build \
	    dist \
	    composable_logs.egg-info \
	    composable_logs_snapshot.egg-info

build: | clean
	@# TODO: test that environment variables are set
	@echo ">>> Building library wheel file ..."
	@echo " - PYTHON_PACKAGE_RELEASE_TARGET  : ${PYTHON_PACKAGE_RELEASE_TARGET}"
	@echo " - GITHUB_SHA                     : ${GITHUB_SHA}"
	@echo " - README_FILEPATH                : ${README_FILEPATH}"
	@python3 setup.py bdist_wheel
