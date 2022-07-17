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
"""Schemas for Reader and Writer configs."""
from enum import Enum

from marshmallow import Schema
from marshmallow import ValidationError
from marshmallow import fields
from marshmallow import post_load

from dwh_migration_client.io.file import FileReader, FileWriter


class ReaderType(Enum):
    """An Enum representing Reader types."""

    FILE = FileReader


class ReaderSchema(Schema):
    """Reader config schema."""

    type = fields.Method(required=True, deserialize='_deserialize_reader_type')

    @staticmethod
    def _deserialize_reader_type(obj: str) -> ReaderType:
        for member in ReaderType:
            if member.name == obj.upper():
                return member
        raise ValidationError(
            f"{obj} is not a valid reader type. "
            f"Valid reader types are: {[m.name.lower() for m in ReaderType]}")

    include = fields.List(fields.String(), required=True)
    # marshmallow.Schema already has a name "exclude", thus the underscore.
    _exclude = fields.List(fields.String(), data_key='exclude', load_default=[])

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return data['type'].value.from_config(data['include'], data['_exclude'])


class WriterType(Enum):
    """An Enum representing Writer types."""

    FILE = FileWriter


class WriterSchema(Schema):
    """Writer config schema."""

    type = fields.Method(required=True, deserialize='_deserialize_writer_type')

    @staticmethod
    def _deserialize_writer_type(obj: str) -> WriterType:
        for member in WriterType:
            if member.name == obj.upper():
                return member
        raise ValidationError(
            f"{obj} is not a valid writer type. "
            f"Valid writer types are: {[m.name.lower() for m in WriterType]}")

    @post_load
    def build(self, data, **kwargs):  # type: ignore[no-untyped-def] # pylint: disable=unused-argument
        return data['type'].value()
