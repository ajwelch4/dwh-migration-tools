SHELL := /bin/bash -ex

.PHONY: format
format:
	black .
	isort .

.PHONY: lint
lint: format
	pylint *.py

.PHONY: check
check: lint
	mypy --strict .