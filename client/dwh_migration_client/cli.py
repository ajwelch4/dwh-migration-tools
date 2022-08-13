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
"""CLI for BigQuery Batch SQL Translator."""

import argparse
import logging
import pathlib
import shutil
from functools import partial
from typing import List


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
        f"{path.as_posix()} is not a regular file that exists."
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
        f"{path.as_posix()} is not a directory that exists."
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

    raise argparse.ArgumentTypeError(f"{path.as_posix()} already exists.")


def parse_args(args: List[str]) -> argparse.Namespace:
    """Argument parser for the BigQuery Batch SQL Translator CLI."""
    parser = argparse.ArgumentParser(
        description="Config the Batch Sql translation tool."
    )
    parser.add_argument(
        "--verbose", help="Increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "--config",
        type=validated_file,
        default="config/config.yaml",
        help="Path to the config.yaml file. (default: config/config.yaml)",
    )
    parser.add_argument(
        "--input",
        type=validated_directory,
        default="input",
        help="Path to the input_directory. (default: input)",
    )
    parser.add_argument(
        "--output",
        type=partial(validated_nonexistent_path, force=True),
        default="output",
        help="Path to the output_directory. (default: output)",
    )
    parser.add_argument(
        "-m",
        "--macros",
        type=validated_file,
        help="Path to the macro map yaml file. If specified, the program will "
             "pre-process all the input query files by replacing the macros with "
             "corresponding string values according to the macro map definition. After "
             "translation, the program will revert the substitutions for all the output "
             "query files in a post-processing step.  The replacement does not apply for "
             "files with extension of .zip, .csv, .json.",
    )
    parser.add_argument(
        "-o",
        "--object_name_mapping",
        type=validated_file,
        help="Path to the object name mapping json file. Name mapping lets you "
             "identify the names of SQL objects in your source files, and specify target "
             "names for those objects in BigQuery. More info please see "
             "https://cloud.google.com/bigquery/docs/output-name-mapping.",
    )
    parser.add_argument(
        "-p",
        "--processors",
        type=validated_file,
        help="Path to the processors Python file. This file should contain a "
             "function called preprocess that gets called once for each input "
             "file as well as a function called postprocess that gets called "
             "once for each translated output file. See config/processors.py "
             "for example.",
    )

    parsed_args = parser.parse_args(args)

    logging.basicConfig(
        level=logging.DEBUG if parsed_args.verbose else logging.INFO,
        format="%(asctime)s: %(levelname)s: %(message)s",
    )

    return parsed_args
