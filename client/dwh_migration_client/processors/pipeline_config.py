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
"""A parser for the processor pipeline config file."""

import importlib
import logging
import pathlib
import shutil
from dataclasses import asdict, dataclass
from pprint import pformat
from typing import List, Optional, Type, cast

import yaml
from marshmallow import Schema, ValidationError, fields, post_load
from yaml import SafeLoader

from dwh_migration_client.processors.abstract_processor import Processor


class ProcessorSchema(Schema):
    """Schema and data validator for Processors."""

    name = fields.Method(required=True, deserialize="_deserialize_name")

    @staticmethod
    def _deserialize_name(obj: str) -> Type[Processor]:
        processor_name_parts = obj.rpartition(".")
        processor_module_name = processor_name_parts[0]
        processor_class_name = processor_name_parts[2]
        processor_module = importlib.import_module(processor_module_name)
        processor_class = getattr(processor_module, processor_class_name)
        if not issubclass(processor_class, Processor):
            raise ValidationError(
                f"{obj} must be a concrete subclass of "
                "dwh_migration_client.processors.abstract_processor.Processor."
            )
        # TODO: Determine why type narrowing is not occurring.
        #       https://github.com/python/mypy/issues/10680
        return cast(Type[Processor], processor_class)

    args = fields.Dict(keys=fields.String(), values=fields.Raw(), required=True)

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        processor_class = data["name"]
        processor_instance = processor_class.from_args(data["args"])
        return processor_instance


@dataclass
class ProcessorPipelineConfig:
    """A structure for holding the processor pipeline config."""

    processors: List[Processor]
    intermediate_directory: Optional[pathlib.Path] = None


class ProcessorPipelineConfigSchema(Schema):
    """Schema and data validator for ProcessorPipelineConfig."""

    processors = fields.List(fields.Nested(ProcessorSchema), required=True)
    intermediate_directory = fields.Method(
        deserialize="_deserialize_intermediate_directory"
    )

    @staticmethod
    def _deserialize_intermediate_directory(obj: str) -> pathlib.Path:
        intermediate_directory = pathlib.Path(obj).resolve()
        if intermediate_directory.exists():
            if intermediate_directory.is_dir():
                shutil.rmtree(intermediate_directory)
            else:
                raise ValidationError(
                    f"{intermediate_directory} exists and is not a directory."
                )
        return intermediate_directory

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return ProcessorPipelineConfig(**data)


def parse(config_file_path: pathlib.Path) -> ProcessorPipelineConfig:
    """Parses the config file into a ProcessorPipelineConfig object.

    Return:
        ProcessorPipelineConfig object.
    """
    logging.info("Parsing processor pipeline config file: %s.", config_file_path)
    with open(config_file_path, encoding="utf-8") as file:
        data = yaml.load(file, Loader=SafeLoader)
    try:
        config: ProcessorPipelineConfig = ProcessorPipelineConfigSchema().load(data)
    except ValidationError as error:
        logging.error(
            "Invalid processor pipeline config file: %s: %s.", config_file_path, error
        )
        raise
    logging.info(
        "Finished parsing processor pipeline config file: %s:\n%s.",
        config_file_path,
        pformat(asdict(config)),
    )
    return config
