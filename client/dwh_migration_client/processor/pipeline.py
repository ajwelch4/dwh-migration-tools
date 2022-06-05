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

import os
import shutil
from os.path import abspath, dirname, isfile, join

from dwh_migration_client.processor.pipeline_config_parser import (
    ProcessorPipelineConfig,
)


class ProcessorPipeline:
    """A class that handles files and marshals them through a processor pipeline."""

    def __init__(
        self,
        config: ProcessorPipelineConfig,
        input_path: str,
        output_path: str,
    ) -> None:
        # The name of a hidden directory that stores temporary processed files.
        # This directory will be deleted (by default) after the job finishes.
        self._tmp_directory = join(dirname(input_path), ".tmp")
        # User-supplied input directory
        self._input_path = abspath(input_path)
        # Tmp directory where preprocessed files will be placed for upload.
        self.upload_path = join(self._tmp_directory, "upload")
        # User-supplied output directory
        self._output_path = abspath(output_path)
        # Tmp directory where files will be placed after download.
        self.download_path = join(self._tmp_directory, "download")
        self._processors = config.processors
        self._clean_up_tmp_files = config.clean_up_tmp_files

    def preprocess(self) -> None:
        """The pre-upload entry point of ProcessorPipeline."""
        self.__process(
            self._input_path,
            self.upload_path,
            revert_expansion=False,
        )

    def postprocess(self) -> None:
        """The post-download entry point of ProcessorPipeline."""
        self.__process(
            self.download_path,
            self._output_path,
            revert_expansion=True,
        )
        if self._clean_up_tmp_files:
            print('Cleaning up tmp files under "%s"...' % self._tmp_directory)
            shutil.rmtree(self._tmp_directory)
            print("Finished cleanup.")

    def is_ignored(self, path: str, name: str) -> bool:
        """Returns true if a file is ignored.

        Ignored files are not transpiled or copied to the output directory.
        """
        if not isfile(path):
            return True
        if name.startswith("."):
            return True
        return False

    def is_processable(self, path: str, name: str) -> bool:
        """Returns true if a file is preprocessable.

        Preprocessable files are subject to macro preprocessing and (optionally)
        postprocessing. Non-preprocessable files are transpiled verbatim. To ignore a
        file entirely, modify is_ignored.
        """
        if self.is_ignored(path, name):
            return False
        if name.lower().endswith((".zip", ".json", ".csv")):
            return False
        return True

    def __process(
        self, input_dir: str, output_dir: str, revert_expansion: bool = False
    ) -> None:
        """Replaces or restores macros for every file in the input folder and save
        outputs in a new folder.

        Macro replacement doesn't apply for files which are ignored, or not processable.
        Note that this method is called for varying combinations of input and output
        directories at different points in the process.

        Args:
            input_dir: absolute path to the input directory.
            output_dir: absolute path to the output directory.
            revert_expansion: whether to revert the macro substitution.
        """
        for root, _, files in os.walk(input_dir):
            for name in files:
                sub_dir = root[len(input_dir) + 1 :]
                input_path = join(input_dir, sub_dir, name)
                output_path = join(output_dir, sub_dir, name)
                if self.is_ignored(input_path, name):
                    continue
                os.makedirs(dirname(output_path), exist_ok=True)
                if not self.is_processable(input_path, name):
                    shutil.copy(input_path, output_path)
                    continue
                # The user may implement entirely different logic for macro expansion
                # vs. unexpansion, especially if they are migrating between systems,
                # so we use a boolean flag to separate the paths again here.
                if not revert_expansion:
                    self.preprocess_file(input_path, output_path, input_dir)
                else:
                    self.postprocess_file(input_path, output_path, output_dir)

    def preprocess_file(
        self, input_path: str, upload_path: str, input_dir: str
    ) -> None:
        """Replaces macros for the input file and save the output file in a tmp path.

        Args:
            input_path: absolute path to the input file.
            upload_path: absolute path to the output tmp file.
            input_dir: absolute path to the input directory. The input file can be in a
                subdirectory in the input_dir.
        """
        print("Preprocessing %s" % input_path)
        with open(input_path, encoding="utf-8") as input_fh:
            text = input_fh.read()
        for processor in self._processors:
            text = processor.preprocess(input_path[len(input_dir) + 1 :], text)
        with open(upload_path, "w", encoding="utf-8") as tmp_fh:
            tmp_fh.write(text)

    def postprocess_file(
        self, download_path: str, output_path: str, output_dir: str
    ) -> None:
        """Postprocesses the given file, after conversion to the target dialect.

        Args:
            download_path: absolute path to the tmp file.
            output_path: absolute path to the output file after postprocessing.
            output_dir: absolute path to the output directory. The output file can be in
                a subdirectory in the output_dir.
        """
        print("Postprocessing into %s" % output_path)
        with open(download_path, encoding="utf-8") as tmp_fh:
            text = tmp_fh.read()
        for processor in reversed(self._processors):
            text = processor.postprocess(output_path[len(output_dir) + 1 :], text)
        with open(output_path, "w", encoding="utf-8") as output_fh:
            output_fh.write(text)
