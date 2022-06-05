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
"""CLI for BigQuery Batch SQL Translator"""

import argparse
import os
import pathlib
import shutil
import sys
from functools import partial
from importlib import resources

from dwh_migration_client import batch_sql_translator
from dwh_migration_client.config_parser import ConfigParser
from dwh_migration_client.gcloud_auth_helper import validate_gcloud_auth_settings
from dwh_migration_client.gcs_helper import GcsHelper
from dwh_migration_client.object_mapping_parser import ObjectMappingParser
from dwh_migration_client.processor.pipeline import ProcessorPipeline
from dwh_migration_client.processor.pipeline_config_parser import (
    ProcessorPipelineConfigParser,
)


def init(args: argparse.Namespace) -> None:
    """Initializes a new batch SQL translation project."""
    project_directory = os.path.abspath(args.directory)

    print("Initializing a new batch SQL translation project in %s" % project_directory)
    with resources.path("dwh_migration_client", "example") as example:
        shutil.copytree(example, project_directory)

    project_processors_directory = pathlib.Path(
        os.path.join(project_directory, "processors")
    )
    project_processors_directory.mkdir()

    processor_package = "dwh_migration_client.processor"

    init_file_name = "__init__.py"
    with resources.path(processor_package, init_file_name) as init_file:
        shutil.copyfile(
            init_file, os.path.join(project_processors_directory, init_file_name)
        )

    macro_processor_file_name = "macro_processor.py"
    with resources.path(
        processor_package, macro_processor_file_name
    ) as macro_processor_file:
        shutil.copyfile(
            macro_processor_file,
            os.path.join(project_processors_directory, macro_processor_file_name),
        )

    print("Initial project scaffolding has been created in %s" % project_directory)


def translate(args: argparse.Namespace) -> None:
    """Starts a batch sql translation job."""
    config = ConfigParser(args.config).parse_config()

    print("\nVerify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.project_number)

    if args.processor_pipeline_config:
        processor_pipeline_config = ProcessorPipelineConfigParser(
            args.processor_pipeline_config
        ).parse()
        processor_pipeline = ProcessorPipeline(
            config=processor_pipeline_config,
            input_path=args.input,
            output_path=args.output,
        )
        upload_path = processor_pipeline.upload_path
        download_path = processor_pipeline.download_path
    else:
        processor_pipeline = None
        upload_path = args.input
        download_path = args.output

    gcs_helper = GcsHelper(
        bucket=config.gcs_bucket,
        local_upload_path=upload_path,
        local_download_path=download_path,
        translation_type=config.translation_type,
    )

    if args.object_name_mapping:
        object_name_mapping_list = ObjectMappingParser(
            args.object_name_mapping
        ).get_name_mapping_list()
    else:
        object_name_mapping_list = None

    translator = batch_sql_translator.BatchSqlTranslator(
        config, gcs_helper, processor_pipeline, object_name_mapping_list
    )
    translator.start_translation()


def validated_file(unvalidated_path: str) -> str:
    """Validates a path is a regular file that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a regular file that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_file():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        "%s is not a regular file that exists." % path.as_posix()
    )


def validated_directory(unvalidated_path: str) -> str:
    """Validates a path is a directory that exists.
    Args:
        unvalidated_path: A string representing the path to validate.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path is not a directory that exists.
    """
    path = pathlib.Path(unvalidated_path)
    if path.is_dir():
        return path.as_posix()
    raise argparse.ArgumentTypeError(
        "%s is not a directory that exists." % path.as_posix()
    )


def validated_nonexistent_path(unvalidated_path: str, force: bool = False) -> str:
    """Validates a path does not exist.
    Args:
        unvalidated_path: A string representing the path to validate.
        force: A boolean representing whether to remove unvalidated_path if it exists.
    Returns:
        A string representing a validated POSIX path.
    Raises:
        argparse.ArgumentTypeError: unvalidated_path already exists.
    """
    path = pathlib.Path(unvalidated_path)

    if not path.exists():
        return path.as_posix()

    if force:
        if path.is_dir():
            shutil.rmtree(path)
        if path.is_file():
            path.unlink()
        return path.as_posix()

    raise argparse.ArgumentTypeError("%s already exists." % path.as_posix())


def main() -> None:
    """CLI for BigQuery Batch SQL Translator"""
    parser = argparse.ArgumentParser(
        description="Config the Batch Sql translation tool."
    )
    subparsers = parser.add_subparsers(title="Subcommands")

    init_subparser = subparsers.add_parser(
        "init", help="Initialize a new batch SQL translation project."
    )
    init_subparser.add_argument(
        "directory",
        type=validated_nonexistent_path,
        help="The directory to create and populate with the initial project "
        "scaffolding.",
    )
    init_subparser.set_defaults(subcommand_handler=init)

    translate_subparser = subparsers.add_parser(
        "translate", help="Execute a batch SQL translation job."
    )
    translate_subparser.add_argument(
        "--config",
        type=validated_file,
        default="config.yaml",
        help="Path to the config.yaml file.",
    )
    translate_subparser.add_argument(
        "--input",
        type=validated_directory,
        default="input",
        help="Path to the input_directory.",
    )
    translate_subparser.add_argument(
        "--output",
        type=partial(validated_nonexistent_path, force=True),
        default="output",
        help="Path to the input_directory.",
    )
    translate_subparser.add_argument(
        "-p",
        "--processor_pipeline_config",
        type=validated_file,
        help="Path to the processors yaml file. If specified, the program will "
        "preprocess all the input query files by passing them to the preprocess "
        "method of each processor classes listed in the processors yaml file. After "
        "translation, the program will postprocess the translated output files by "
        "passing them to the postprocess method of each processor classes listed in "
        "the processors yaml file.  The pre and postprocessing does not apply for "
        "files with extension of .zip, .csv, .json.",
    )
    translate_subparser.add_argument(
        "-o",
        "--object_name_mapping",
        type=validated_file,
        help="Path to the object name mapping json file. Name mapping lets you "
        "identify the names of SQL objects in your source files, and specify target "
        "names for those objects in BigQuery. More info please see "
        "https://cloud.google.com/bigquery/docs/output-name-mapping.",
    )
    translate_subparser.set_defaults(subcommand_handler=translate)

    # Print usage message if no args are supplied.
    if len(sys.argv) <= 1:
        sys.argv.append("--help")

    args = parser.parse_args()
    args.subcommand_handler(args)


if __name__ == "__main__":
    main()
