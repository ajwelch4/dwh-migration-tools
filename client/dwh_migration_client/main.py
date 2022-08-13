# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Instantiate the object graph, preprocess, translate and postprocess."""

import argparse
import logging
import sys

from marshmallow import ValidationError

from dwh_migration_client.gcp.bqms import batch_sql_translator
from dwh_migration_client.config import parse as parse_config
from dwh_migration_client.gcp.gcloud_auth_helper import validate_gcloud_auth_settings
from dwh_migration_client.macro_processor import MacroProcessor
from dwh_migration_client.gcp.bqms.object_name_mapping import parse as parse_object_name_mapping
from dwh_migration_client.cli import parse_args


def start_translation(args: argparse.Namespace) -> None:
    """Starts a batch sql translation job."""
    try:
        config = parse_config(args.config)
    except ValidationError:
        sys.exit(1)

    if args.object_name_mapping:
        try:
            object_name_mapping_list = parse_object_name_mapping(
                args.object_name_mapping
            )
        except ValidationError:
            sys.exit(1)
    else:
        object_name_mapping_list = None

    if args.macros:
        try:
            preprocessor = MacroProcessor(args)
        except ValidationError:
            sys.exit(1)
    else:
        preprocessor = None

    logging.info("Verify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.gcp_settings.project_number)

    translator = batch_sql_translator.BatchSqlTranslator(
        config, args.input, args.output, preprocessor, object_name_mapping_list
    )
    translator.start_translation()


def main() -> None:
    """Instantiate the object graph, preprocess, translate and postprocess."""
    args = parse_args(sys.argv[1:])
    return start_translation(args)


if __name__ == "__main__":
    main()
