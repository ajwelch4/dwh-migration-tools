# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Performance tests."""
import cProfile
import io
import logging
import os
import pstats
import random
import shutil
import string
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from dwh_migration_client.main import parse_args, start_translation


@pytest.fixture(scope="module")
def project_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("project")
    yield tmp_path
    shutil.rmtree(str(tmp_path))


@pytest.fixture(scope="module", params=[10, 10**4, 10**5])
def number_of_files(request, pytestconfig):
    if not pytestconfig.getoption("performance", default=False) and request.param != 10:
        pytest.skip()
    return request.param


def test_teradata(project_path, number_of_files):
    """Performance test."""
    input_path = project_path / "input"
    input_path.mkdir()

    config_path = project_path / "config.yaml"
    with open(config_path, "w", encoding="utf-8") as config_file:
        config_file.write("gcp_settings:\n")
        config_file.write(f"  project_number: '{os.getenv('BQMS_PROJECT')}'\n")
        config_file.write(f"  gcs_bucket: '{os.getenv('BQMS_GCS_BUCKET')}'\n")
        config_file.write("translation_config:\n")
        config_file.write("  translation_type: Translation_Teradata2BQ\n")
        config_file.write("  location: us\n")

    macro_mapping_path = project_path / "macros.yaml"
    with open(macro_mapping_path, "w", encoding="utf-8") as macro_mapping_file:
        macro_mapping_file.write("macros:\n")
        macro_mapping_file.write("  '*.sql':\n")
        macro_mapping_file.write("    '${foo}': '1'\n")

    object_name_mapping_path = project_path / "object_mapping.json"
    with open(object_name_mapping_path, "w", encoding="utf-8") as object_name_mapping_file:
        object_name_mapping_file.write('{\n')
        object_name_mapping_file.write('  "name_map": [\n')
        object_name_mapping_file.write('    {\n')
        object_name_mapping_file.write('        "source": {\n')
        object_name_mapping_file.write('            "type": "RELATION",\n')
        object_name_mapping_file.write('            "database": "__DEFAULT_DATABASE__",\n')
        object_name_mapping_file.write('            "schema": "__DEFAULT_SCHEMA__",\n')
        object_name_mapping_file.write('            "relation": "test"\n')
        object_name_mapping_file.write('        },\n')
        object_name_mapping_file.write('        "target": {\n')
        object_name_mapping_file.write('            "database": "bq_project",\n')
        object_name_mapping_file.write('            "schema": "bq_dataset",\n')
        object_name_mapping_file.write('            "relation": "test_foo"\n')
        object_name_mapping_file.write('        }\n')
        object_name_mapping_file.write('    }\n')
        object_name_mapping_file.write('  ]\n')
        object_name_mapping_file.write('}\n')

    def _write_test_file(i):
        """Generate a ~10 KB file."""
        input_file_name = input_path / f"test_{i:06}.sql"
        with open(input_file_name, "w", encoding="utf-8") as input_file:
            input_file.write("create table test (a integer);\n")
            input_file.write("select * from test where a = ${foo};\n")
            for _ in range(10**2):
                random_string = "".join(
                    random.choices(string.ascii_letters + string.digits, k=10**2)
                )
                input_file.write(f"-- {random_string}\n")

    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(number_of_files):
            futures.append(executor.submit(_write_test_file, i))

        # Trigger any exceptions.
        for future in as_completed(futures):
            future.result()
        futures.clear()

    profile = cProfile.Profile()
    profile.enable()

    args = parse_args(
        [
            "--config",
            str(config_path),
            "--input",
            str(input_path),
            "--output",
            str(project_path / "output"),
            "--macros",
            str(macro_mapping_path),
            "--object_name_mapping",
            str(object_name_mapping_path)
        ]
    )
    start_translation(args)

    profile.disable()
    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream).sort_stats("tottime")
    stats.print_stats(25)
    logging.info("cProfile stats:")
    logging.info(stream.getvalue())
