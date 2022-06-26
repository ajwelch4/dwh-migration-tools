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
"""An abstract base class for all processors to inherit from."""

import pathlib
from abc import ABCMeta, abstractmethod


class Processor(metaclass=ABCMeta):
    @abstractmethod
    def preprocess(self, relative_path: pathlib.Path, text: str) -> str:
        ...

    @abstractmethod
    def postprocess(self, relative_path: pathlib.Path, text: str) -> str:
        ...
