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
"""A processor to handle macros in the query files during the pre-processing and
post-processing stages of a Batch Sql Translation job.
"""

import fnmatch
import logging
import pathlib
import re
from pprint import pformat
from typing import Dict, Pattern, Tuple

import yaml
from marshmallow import Schema, ValidationError, fields
from yaml.loader import SafeLoader


class MacrosSchema(Schema):
    macros = fields.Dict(
        keys=fields.String(),
        values=fields.Dict(keys=fields.String(), values=fields.String(), required=True),
        required=True,
    )


class MacroProcessor:
    """A processor to handle macros in the query files during the pre-processing and
    post-processing stages of a Batch Sql Translation job.
    """

    def __init__(self, yaml_file_path: str) -> None:
        self.yaml_file_path = yaml_file_path
        self.macro_expansion_maps = self._parse_macros_config_file()
        self.reversed_maps = self._get_reversed_maps()

    def expand(self, text: str, path: pathlib.Path) -> str:
        """Expands the macros in the text with the corresponding values defined in the
        macros_substitution_map file.

        Returns the text after macro substitution.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(path.as_posix())
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def unexpand(self, text: str, path: pathlib.Path) -> str:
        """Reverts the macros substitution by replacing the values with macros defined
        in the macros_substitution_map file.

        Returns the text after replacing the values with macros.
        """
        reg_pattern_map, patterns = self._get_all_regex_pattern_mapping(path.as_posix(), True)
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], text)

    def _get_reversed_maps(self) -> Dict[str, Dict[str, str]]:
        """Swaps key and value in the macro maps and return the new map."""
        reversed_maps = {}
        for file_key, macro_map in self.macro_expansion_maps.items():
            reversed_maps[file_key] = dict((v, k) for k, v in macro_map.items())
        return reversed_maps

    def _parse_macros_config_file(self) -> Dict[str, Dict[str, str]]:
        """Parses the macros mapping yaml file.

        Return:
            macros_replacement_maps: mapping from macros to the replacement string for
                each file.  {file_name: {macro: replacement}}. File name supports
                wildcard, e.g., with "*.sql", the method will apply the macro map to all
                the files with extension of ".sql".
        """
        logging.info("Parsing macros file: %s.", self.yaml_file_path)
        with open(self.yaml_file_path, encoding="utf-8") as file:
            data = yaml.load(file, Loader=SafeLoader)
        try:
            validated_data: Dict[str, Dict[str, Dict[str, str]]] = MacrosSchema().load(
                data
            )
        except ValidationError as error:
            logging.error("Invalid macros file: %s: %s.", self.yaml_file_path, error)
            raise
        logging.info(
            "Finished parsing macros file: %s:\n%s.",
            self.yaml_file_path,
            pformat(validated_data),
        )
        return validated_data["macros"]

    def _get_all_regex_pattern_mapping(
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
