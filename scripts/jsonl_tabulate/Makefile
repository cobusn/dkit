PYTHON ?= python3
PIP ?= $(PYTHON) -m pip

.PHONY: install install-test test build clean distclean

install:
	$(PIP) install .

install-test:
	$(PIP) install .[test]

test:
	$(PYTHON) -m pytest -q tests/test_jsonl_tabulate.py

build:
	$(PIP) wheel . -w dist

clean:
	rm -rf build dist *.egg-info .pytest_cache __pycache__ tests/__pycache__

distclean: clean
	rm -rf .tox .venv
