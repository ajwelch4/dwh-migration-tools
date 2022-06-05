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
import os.path
import sys
from dataclasses import dataclass
from os.path import abspath
from typing import Dict, List, Tuple

import yaml
from yaml import SafeLoader

from dwh_migration_client.processor.abstract_processor import Processor


@dataclass
class ProcessorPipelineConfig:
    """A structure for holding the processor pipeline config."""

    processors: List[Processor]
    clean_up_tmp_files: bool


class ProcessorPipelineConfigParser:  # pylint: disable=too-few-public-methods
    """A parser for the processor pipeline config file."""

    def __init__(self, config_file_path: str) -> None:
        self._config_file_path = abspath(config_file_path)

    # Config field name
    __PROCESSORS = "processors"
    __PROCESSOR_CLASS = "class"
    __PROCESSOR_CLASS_ARGS = "args"
    __CLEAN_UP = "clean_up_tmp_files"

    def _processor_configs(self, data: Dict[str, object]) -> List[Dict[str, object]]:
        assert self.__PROCESSORS in data, "Root field missing: %s." % self.__PROCESSORS
        processor_configs = data[self.__PROCESSORS]
        assert isinstance(processor_configs, list), (
            "Root field %s must be a list." % self.__PROCESSORS
        )
        return processor_configs

    def _processor_name_parts(
        self, processor_config: Dict[str, object]
    ) -> Tuple[str, str]:
        assert self.__PROCESSOR_CLASS in processor_config, "%s field missing: %s." % (
            self.__PROCESSORS,
            self.__PROCESSOR_CLASS,
        )
        qualified_processor_class_name = processor_config[self.__PROCESSOR_CLASS]
        assert isinstance(
            qualified_processor_class_name, str
        ), "%s field %s must be a string." % (
            self.__PROCESSORS,
            self.__PROCESSOR_CLASS,
        )
        processor_name_parts = qualified_processor_class_name.rpartition(".")
        return processor_name_parts[0], processor_name_parts[2]

    def _processor_class_args(
        self, processor_config: Dict[str, object]
    ) -> Dict[str, object]:
        if self.__PROCESSOR_CLASS_ARGS in processor_config:
            processor_class_args = processor_config[self.__PROCESSOR_CLASS_ARGS]
            assert isinstance(
                processor_class_args, dict
            ), "%s field %s must be a dictionary." % (
                self.__PROCESSORS,
                self.__PROCESSOR_CLASS_ARGS,
            )
        else:
            processor_class_args = {}
        return processor_class_args

    def _processor_instance(
        self,
        qualified_processor_module_name: str,
        processor_class_name: str,
        processor_class_args: Dict[str, object],
    ) -> Processor:
        processor_module = importlib.import_module(qualified_processor_module_name)
        processor_class = getattr(processor_module, processor_class_name)
        assert issubclass(
            processor_class, Processor
        ), "%s field %s must be a string that represents a Processor class." % (
            self.__PROCESSORS,
            self.__PROCESSOR_CLASS,
        )
        processor_instance = processor_class(**processor_class_args)
        assert isinstance(
            processor_instance, Processor
        ), "%s field %s must be a string that represents a Processor class." % (
            self.__PROCESSORS,
            self.__PROCESSOR_CLASS,
        )
        return processor_instance

    def _clean_up_tmp_files(self, data: Dict[str, object]) -> bool:
        if self.__CLEAN_UP in data:
            clean_up_tmp_files = data[self.__CLEAN_UP]
            assert isinstance(clean_up_tmp_files, bool), (
                "Root field %s must be a boolean." % self.__CLEAN_UP
            )
        else:
            clean_up_tmp_files = True
        return clean_up_tmp_files

    def parse(self) -> ProcessorPipelineConfig:
        """Parses the config file into ProcessorPipelineConfig.

        Return:
            ProcessorPipelineConfig.
        """
        # Ensure cwd is on sys.path so user supplied processors can be imported.
        if os.getcwd() not in sys.path:
            sys.path.insert(0, os.getcwd())

        print("Reading processors config file from %s..." % self._config_file_path)
        with open(self._config_file_path, encoding="utf-8") as file:
            data: Dict[str, object] = yaml.load(file, Loader=SafeLoader)

        processors: List[Processor] = []
        for processor_config in self._processor_configs(data):
            (
                processor_module_name,
                processor_class_name,
            ) = self._processor_name_parts(processor_config)
            processor_class_args = self._processor_class_args(processor_config)
            processor_instance = self._processor_instance(
                processor_module_name,
                processor_class_name,
                processor_class_args,
            )
            processors.append(processor_instance)

        clean_up_tmp_files = self._clean_up_tmp_files(data)

        config = ProcessorPipelineConfig(
            processors=processors,
            clean_up_tmp_files=clean_up_tmp_files,
        )

        print("Finished parsing processors config.")
        print("The config is:")
        print("\n".join("     %s: %s" % item for item in vars(config).items()))
        return config
