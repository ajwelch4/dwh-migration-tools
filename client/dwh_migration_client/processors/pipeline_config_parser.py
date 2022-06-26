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
import sys
from dataclasses import asdict, dataclass
from pprint import pformat
from typing import Dict, List, Optional, Tuple

import yaml
from yaml import SafeLoader

from dwh_migration_client.processors.abstract_processor import Processor


@dataclass
class ProcessorPipelineConfig:
    """A structure for holding the processor pipeline config."""

    processors: List[Processor]
    intermediate_directory: Optional[pathlib.Path]


class ProcessorPipelineConfigParser:  # pylint: disable=too-few-public-methods
    """A parser for the processor pipeline config file."""

    def __init__(self, config_file_path: str) -> None:
        self._config_file_path = config_file_path

    # Config field name
    _PROCESSORS = "processors"
    _PROCESSOR_CLASS = "class"
    _PROCESSOR_CLASS_ARGS = "args"
    _INTERMEDIATE_DIRECTORY = "intermediate_directory"

    def _processor_configs(self, data: Dict[str, object]) -> List[Dict[str, object]]:
        if self._PROCESSORS not in data:
            raise ValueError(f"Root field missing: {self._PROCESSORS}.")
        processor_configs = data[self._PROCESSORS]
        if not isinstance(processor_configs, list):
            raise ValueError(f"Root field {self._PROCESSORS} must be a list.")
        return processor_configs

    def _processor_name_parts(
        self, processor_config: Dict[str, object]
    ) -> Tuple[str, str]:
        if self._PROCESSOR_CLASS not in processor_config:
            raise ValueError(
                f"{self._PROCESSORS} field missing: {self._PROCESSOR_CLASS}."
            )
        qualified_processor_class_name = processor_config[self._PROCESSOR_CLASS]
        if not isinstance(qualified_processor_class_name, str):
            raise ValueError(
                f"{self._PROCESSORS} field {self._PROCESSOR_CLASS} must be a string."
            )
        processor_name_parts = qualified_processor_class_name.rpartition(".")
        return processor_name_parts[0], processor_name_parts[2]

    def _processor_class_args(
        self, processor_config: Dict[str, object]
    ) -> Dict[str, object]:
        if self._PROCESSOR_CLASS_ARGS in processor_config:
            processor_class_args = processor_config[self._PROCESSOR_CLASS_ARGS]
            if not isinstance(processor_class_args, dict):
                raise ValueError(
                    f"{self._PROCESSORS} field "
                    f"{self._PROCESSOR_CLASS_ARGS} must be a dictionary."
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
        if not issubclass(processor_class, Processor):
            raise ValueError(
                f"{self._PROCESSORS} field {self._PROCESSOR_CLASS} "
                "must be a string that represents a Processor class."
            )
        processor_instance = processor_class(**processor_class_args)
        if not isinstance(processor_instance, Processor):
            raise ValueError(
                f"{self._PROCESSORS} field {self._PROCESSOR_CLASS} "
                "must be a string that represents a Processor class."
            )
        return processor_instance

    def _intermediate_directory(
        self, data: Dict[str, object]
    ) -> Optional[pathlib.Path]:
        intermediate_directory = None
        if self._INTERMEDIATE_DIRECTORY in data:
            raw_intermediate_directory = data[self._INTERMEDIATE_DIRECTORY]
            if not isinstance(raw_intermediate_directory, str):
                raise ValueError(
                    f"Root field {self._INTERMEDIATE_DIRECTORY} must be a string."
                )
            intermediate_directory = pathlib.Path(raw_intermediate_directory).resolve()
            if intermediate_directory.exists():
                if intermediate_directory.is_dir():
                    shutil.rmtree(intermediate_directory)
                else:
                    raise ValueError(
                        f"{self._INTERMEDIATE_DIRECTORY} exists and is not a directory."
                    )
        return intermediate_directory

    def parse(self) -> ProcessorPipelineConfig:
        """Parses the config file into ProcessorPipelineConfig.
        Return:
            ProcessorPipelineConfig.
        """
        # Ensure cwd is on sys.path so user supplied processors can be imported.
        processors: List[Processor] = []
        intermediate_directory = None
        if self._config_file_path:
            if pathlib.Path.cwd().as_posix() not in sys.path:
                sys.path.insert(0, pathlib.Path.cwd().as_posix())

            logging.info(
                "Reading processors config file from %s...", self._config_file_path
            )
            with open(self._config_file_path, encoding="utf-8") as file:
                data: Dict[str, object] = yaml.load(file, Loader=SafeLoader)

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

            intermediate_directory = self._intermediate_directory(data)

        config = ProcessorPipelineConfig(
            processors=processors,
            intermediate_directory=intermediate_directory,
        )

        logging.info("Finished parsing processors config.")
        logging.info("The config is:\n%s", pformat(asdict(config)))
        return config
