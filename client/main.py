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


import argparse
import batch_sql_translator

from config_parser import ConfigParser, validate_args, DEFAULT_CONFIG_PATH, DEFAULT_INPUT_PATH, DEFAULT_OUTPUT_PATH
from gcloud_auth_helper import validate_gcloud_auth_settings
from macro_processor import MacroProcessor

parser = argparse.ArgumentParser(description='Config the Batch Sql translation tool.')
parser.add_argument('-m', '--macros', type=str,
                    help='Path to the macro map yaml file. If specified, the program will pre-process '
                         'all the input query files by replacing the macros with corresponding '
                         'string values according to the macro map definition. After translation, '
                         'the program will revert the substitutions for all the output query files in a '
                         'post-processing step.  The replacement does not apply for files with extension of '
                         '.zip, .csv, .json.')
parser.add_argument('--config', type=str, help='Path to the config.yaml file. By default, the tool tries to read from '
                                               '\"%s\".' % DEFAULT_CONFIG_PATH)
parser.add_argument('--input', type=str, help='Path to the input_directory. By default, the tool tries to use the '
                                              'directory at \"%s\".' % DEFAULT_INPUT_PATH)
parser.add_argument('--output', type=str, help='Path to the input_directory. By default, the tool tries to use the '
                                               'directory at \"%s\".' % DEFAULT_OUTPUT_PATH)

args = parser.parse_args()


def start_translation():
    """Starts a batch sql translation job.
    """
    validate_args(args)
    config = ConfigParser(args).parse_config()
    print("\nVerify cloud login and credential settings...")
    validate_gcloud_auth_settings(config.project_number)
    if args.macros:
        preprocessor = MacroProcessor(args)
    else:
        preprocessor = None
    translator = batch_sql_translator.BatchSqlTranslator(config, preprocessor)
    translator.start_translation()


if __name__ == '__main__':
    start_translation()
