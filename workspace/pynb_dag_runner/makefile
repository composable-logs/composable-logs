SHELL := /bin/bash

ifdef WATCH_MODE
  $(info ">> Running unit tests in watch mode ...")
  CMD_PREFIX := find . | grep ".py" | entr
else
  CMD_PREFIX :=
endif

.PHONY: build clean

test-pytest:
	echo ">> Running unit tests ..."
	$(CMD_PREFIX) pytest \
	    -k "$(PYTEST_FILTER)" \
	    --durations=10 \
	    --continue-on-collection-errors tests

test-mypy:
	echo ">> Running mypy type tests ..."
	$(CMD_PREFIX) mypy --ignore-missing-imports --exclude build .

test-black:
	echo ">> Verify that code is black formatted ..."
	$(CMD_PREFIX) black --check --diff .

build:
	@echo ">> Building library wheel file ..."
	@echo "- PYTHON_PACKAGE_RELEASE_TARGET  : $(PYTHON_PACKAGE_RELEASE_TARGET)"
	@echo "- GITHUB_SHA                     : $(GITHUB_SHA)"
	make clean

	python3 setup.py bdist_wheel

clean:
	rm -rf build dist pynb_dag_runner.egg-info
