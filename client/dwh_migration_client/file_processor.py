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
"""FileProcessor class for marshaling files through pre and postprocessing."""
import logging
import pathlib
import shutil
from typing import Optional

from dwh_migration_client.io.abc import Reader, Writer
from dwh_migration_client.macro_processor import MacroProcessor


class FileProcessor:

    def __init__(self, input_path: str, output_path: str, clean_up_tmp_files: bool, reader: Reader, writer: Writer, macro_processor: Optional[MacroProcessor] = None) -> None:
        self._input_path = pathlib.Path(input_path).resolve()
        self._output_path = pathlib.Path(output_path).resolve()
        self._tmp_path = self._input_path.parent / '.tmp'
        self.upload_path = self._tmp_path / 'upload'
        self.download_path = self._tmp_path / 'download'
        self._clean_up_tmp_files = clean_up_tmp_files
        self._reader = reader
        self._macro_processor = macro_processor
        self._writer = writer

    def preprocess(self) -> None:
        logging.info("Preprocessing %s", self._input_path)

        shutil.rmtree(self._tmp_path)
        self.upload_path.mkdir(parents=True)
        self.download_path.mkdir(parents=True)

        for input_file_path in self._input_path.rglob("*"):
            with open(input_file_path, encoding="utf-8") as input_fh:
                text = input_fh.read()

            relative_file_path = input_file_path.relative_to(self._input_path)

            # TODO: deal with ScriptFragments
            reader_processed_text = self._reader.process(relative_file_path, text)
            if not reader_processed_text:
                shutil.copy(input_file_path, self._output_path / relative_file_path)
                continue

            if self._macro_processor:
                macro_processed_text = self._macro_processor.expand(reader_processed_text, relative_file_path)
            else:
                macro_processed_text = reader_processed_text

            upload_file_path = self.upload_path / relative_file_path
            with open(upload_file_path, "w", encoding="utf-8") as upload_fh:
                upload_fh.write(macro_processed_text)

    def postprocess(self) -> None:
        logging.info("Postprocessing into %s", self._output_path)

        for download_file_path in self.download_path.rglob("*"):
            with open(download_file_path, encoding="utf-8") as download_fh:
                text = download_fh.read()

            relative_file_path = download_file_path.relative_to(self.download_path)

            if self._macro_processor:
                macro_processed_text = self._macro_processor.unexpand(text, relative_file_path)
            else:
                macro_processed_text = text

            # TODO: deal with Script
            writer_processed_text = self._writer.process(relative_file_path, macro_processed_text)
            if not writer_processed_text:
                continue

            output_file_path = self._output_path / relative_file_path
            with open(output_file_path, "w", encoding="utf-8") as output_fh:
                output_fh.write(writer_processed_text)

        if self._clean_up_tmp_files and self._tmp_path.exists():
            logging.info('Cleaning up tmp files under "%s"...', self._tmp_path)
            shutil.rmtree(self._tmp_path)
            logging.info("Finished cleanup.")
