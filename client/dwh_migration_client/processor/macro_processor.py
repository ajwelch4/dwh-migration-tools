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
"""A processor for macro variables."""

import fnmatch
import re
from re import Pattern
from typing import Dict, Tuple

from dwh_migration_client.processor.abstract_processor import Processor


class MacroProcessor(Processor):
    """A processor for macro variables."""

    def __init__(self, macros: Dict[str, Dict[str, str]]) -> None:
        self.macro_expansion_maps = macros
        self.reversed_maps = self.__get_reversed_maps()

    def preprocess(self, path: str, text: str) -> str:
        """Expands the macros in the text with the corresponding values defined in the
        self.macro_expansion_maps.

        Returns the text after macro substitution.
        """
        reg_pattern_map, patterns = self.__get_all_regex_pattern_mapping(path)
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def postprocess(self, path: str, text: str) -> str:
        """Reverts the macros substitution by replacing the values with macros defined
        in the self.reversed_maps.

        Returns the text after replacing the values with macros.
        """
        reg_pattern_map, patterns = self.__get_all_regex_pattern_mapping(path, True)
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def __get_reversed_maps(self) -> Dict[str, Dict[str, str]]:
        """Swaps key and value in the macro maps and return the new map."""
        reversed_maps = {}
        for file_key, macro_map in self.macro_expansion_maps.items():
            reversed_maps[file_key] = dict((v, k) for k, v in macro_map.items())
        return reversed_maps

    def __get_all_regex_pattern_mapping(
        self, file_path: str, use_reversed_map: bool = False
    ) -> Tuple[Dict[str, str], Pattern[str]]:
        """Compiles all the macros matched with the file path into a single regex
        pattern."""
        macro_subst_maps = (
            self.reversed_maps if use_reversed_map else self.macro_expansion_maps
        )
        reg_pattern_map = {}
        for file_map_key, token_map in macro_subst_maps.items():
            if fnmatch.fnmatch(file_path, file_map_key):
                for key, value in token_map.items():
                    reg_pattern_map[re.escape(key)] = value
        all_patterns = re.compile("|".join(reg_pattern_map.keys()))
        return reg_pattern_map, all_patterns
