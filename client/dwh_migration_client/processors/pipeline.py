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
"""A class that handles files and marshals them through a processor pipeline."""
import logging
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

from dwh_migration_client.processors.gcs_processor import GcsProcessor
from dwh_migration_client.processors.pipeline_config_parser import (
    ProcessorPipelineConfig,
)


class ProcessorPipeline:
    """A class that handles files and marshals them through a processor pipeline."""

    def __init__(
        self,
        config: ProcessorPipelineConfig,
        input_path: pathlib.Path,
        output_path: pathlib.Path,
        gcs_bucket: str,
    ) -> None:
        self._input_path = input_path
        self._output_path = output_path
        self._processors = config.processors
        gcs_processor = GcsProcessor(bucket=gcs_bucket)
        self._processors.append(gcs_processor)
        self.upload_uri = gcs_processor.upload_uri
        self.download_uri = gcs_processor.download_uri
        self._intermediate_path = config.intermediate_directory
        self._executor = ThreadPoolExecutor()

    @staticmethod
    def is_ignored(path: pathlib.Path) -> bool:
        if not path.is_file():
            return True
        if path.name.startswith("."):
            return True
        return False

    def is_processable(self, path: pathlib.Path) -> bool:
        if self.is_ignored(path):
            return False
        if path.suffix.lower() in (".zip", ".json", ".csv"):
            return False
        return True

    def _process(self, process_func: Callable[[pathlib.Path], None]) -> None:
        futures = []
        for input_file_path in self._input_path.rglob("*"):
            if self.is_ignored(input_file_path):
                continue
            relative_file_path = input_file_path.relative_to(self._input_path)
            futures.append(self._executor.submit(process_func, relative_file_path))
        for future in as_completed(futures):
            future.result()

    def _write_intermediate_file(
        self,
        processor_class: type,
        processor_method: Callable[[pathlib.Path, str], str],
        relative_path: pathlib.Path,
        text: str,
    ) -> None:
        if self._intermediate_path:
            intermediate_processor_path = (
                self._intermediate_path
                / processor_class.__name__
                / processor_method.__name__
                / relative_path
            )
            intermediate_processor_path.parent.mkdir(parents=True, exist_ok=True)
            logging.info("Writing intermediate file: %s", intermediate_processor_path)
            with open(
                intermediate_processor_path, "w", encoding="utf-8"
            ) as intermediate_processor_file:
                intermediate_processor_file.write(text)

    def _preprocess_file(
        self,
        relative_path: pathlib.Path,
    ) -> None:
        input_file_path = self._input_path / relative_path
        logging.info("Preprocessing: %s", input_file_path)
        with open(input_file_path, encoding="utf-8") as input_file:
            text = input_file.read()
        for processor in self._processors:
            text = processor.preprocess(relative_path, text)
            self._write_intermediate_file(
                processor.__class__, processor.preprocess, relative_path, text
            )

    def preprocess(self) -> None:
        self._process(process_func=self._preprocess_file)

    def _postprocess_file(
        self,
        relative_path: pathlib.Path,
    ) -> None:
        output_file_path = self._output_path / relative_path
        logging.info("Postprocessing: %s", output_file_path)
        text = ""
        for processor in reversed(self._processors):
            text = processor.postprocess(relative_path, text)
            self._write_intermediate_file(
                processor.__class__, processor.postprocess, relative_path, text
            )
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file_path, "w", encoding="utf-8") as output_file:
            output_file.write(text)

    def postprocess(self) -> None:
        self._process(process_func=self._postprocess_file)
