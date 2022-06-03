# Install

Install the client into a virtualenv in editable mode along with its
dependencies.

```shell
python3 -m venv venv
source venv/bin/activate
git clone git@github.com:google/dwh-migration-tools.git
pip install -r ./dwh-migration-tools/client/requirements.txt
pip install -e ./dwh-migration-tools/client
```

# Adding and updating dependencies

Add
[abstract dependencies](https://pipenv.pypa.io/en/latest/advanced/#pipfile-vs-setup-py)
to `setup.py` and then run:

```shell
make requirements
```