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
"""Abstract Reader and Writer classes."""

import fnmatch
import pathlib
from abc import ABCMeta, abstractmethod
from typing import List, Optional, Union


class Reader(metaclass=ABCMeta):
    """Abstract Reader.

    Concrete reader implementations are used to handle specific input formats
    such as generic files, ksh, nzsql, etc.
    """

    @classmethod
    @abstractmethod
    def from_config(cls, include: List[str], exclude: List[str]) -> "Reader":
        ...

    @staticmethod
    def _is_processable(relative_path: pathlib.Path, include: List[str], exclude: List[str]) -> bool:
        """Test if an input path should be processed.

        In order to be processable, a path must match at least one include
        glob, and no exclude glob(s)
        """
        for exclude_glob_pattern in exclude:
            if fnmatch.fnmatch(relative_path.as_posix(), exclude_glob_pattern):
                return False
        for include_glob_pattern in include:
            if fnmatch.fnmatch(relative_path.as_posix(), include_glob_pattern):
                return True
        return False


    @abstractmethod
    def process(self, relative_path: pathlib.Path, text: str) -> Optional[Union[str, List[str]]]:
        ...


class Writer(metaclass=ABCMeta):
    """Abstract Writer.

    Concrete writer implementations are used to handle specific output formats
    such as raw files, ksh, nzsql, CSV, etc.
    """

    @abstractmethod
    def process(self, relative_path: pathlib.Path, text: str) -> Optional[str]:
        ...
