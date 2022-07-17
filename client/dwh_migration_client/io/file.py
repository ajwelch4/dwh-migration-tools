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
"""Concrete Reader and Writer classes to handle generic files."""

import pathlib
from typing import List, Optional

from dwh_migration_client.io.abc import Reader, Writer


class FileReader(Reader):
    """FileReader.

    Handles processing of generic input files i.e. files that are not of any
    other type such as ksh. Most often these will be SQL files.
    """

    def __init__(self, include: List[str], exclude: List[str]) -> None:
        self._include = include
        self._exclude = exclude

    @classmethod
    def from_config(cls, include: List[str], exclude: List[str]) -> "FileReader":
        return cls(include, exclude)

    def process(self, relative_path: pathlib.Path, text: str) -> Optional[str]:
        if self._is_processable(relative_path, self._include, self._exclude):
            return text
        return None


class FileWriter(Writer):
    """FileWriter.

    Handles processing of generic output files i.e. files that are not of any
    other type such as ksh. Most often these will be SQL files. Currently, this
    os a noop.
    """

    def process(self, relative_path: pathlib.Path, text: str) -> Optional[str]:
        return text
