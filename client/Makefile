SHELL := /bin/bash -ex

.PHONY: format
format:
	black .
	isort .

.PHONY: lint
lint: format
	pylint *.py