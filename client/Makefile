SHELL := /bin/bash -ex

.PHONY: requirements
requirements:
	$(eval TMP := $(shell mktemp -d))
	python3 -m venv $(TMP) \
		&& source $(TMP)/bin/activate \
		&& pip install . \
		&& pip freeze | sort > requirements.txt \
		&& sed -i '/^dwh-migration-client/d' requirements.txt
	rm -rf $(TMP)