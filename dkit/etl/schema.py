# Copyright (c) 2017 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
Classes and utilities for management of data model schemas.
"""

import collections
import decimal
from typing import Literal, Optional

from dateutil import parser
from pydantic import BaseModel, ConfigDict

from dkit.data import infer


def parse_decimal(value):
    try:
        return decimal.Decimal(value)
    except Exception:
        return None


def parse_datetime(value):
    try:
        return parser.parse(value)
    except Exception:
        return None


def parse_bool(value):
    try:
        return bool(value)
    except Exception:
        return None


def parse_float(value):
    try:
        return float(value)
    except Exception:
        return None


def parse_int(value):
    try:
        return int(value)
    except Exception:
        return None


FieldType = Literal[
    "string", "binary",
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "integer", "boolean", "decimal",
    "float", "double",
    "date", "datetime", "time",
]


class FieldSchema(BaseModel):
    """Pydantic model for a single entity field definition.

    Attributes:
        type: field data type name
        str_len: maximum string length (string fields only)
        scale: decimal scale
        precision: decimal precision
        primary_key: marks field as primary key
        unique: enforce uniqueness constraint
        index: create an index on this field
        computed: field is computed rather than stored
    """

    model_config = ConfigDict(extra="ignore")

    type: FieldType
    str_len: Optional[int] = None
    scale: Optional[int] = None
    precision: Optional[int] = None
    primary_key: Optional[bool] = None
    unique: Optional[bool] = None
    index: Optional[bool] = None
    computed: Optional[bool] = None


class ModelFactory(object):

    def __init__(self, default_str_len=255):
        self.default_str_len = default_str_len


class EntityValidator:
    """
    Entity schema validator.

    Validates field schema definitions using Pydantic and provides
    type coercion utilities.

    Supported field properties:
        - str_len
        - scale / precision
        - primary_key
        - unique
        - index
        - computed

    Args:
        schema_dict: dict mapping field names to field property dicts,
            e.g. {"name": {"type": "string", "str_len": 20}}
    """

    map_python = {
        "boolean": parse_bool,
        "integer": parse_int,
        "float": parse_float,
        "string": str,
        "datetime": parse_datetime,
        "date": parse_datetime,
        "decimal": parse_decimal,
        "binary": bytes,
    }

    # used by `dk schema show_types`
    type_description = {
        "string": "string",
        "binary": "sequence of 8bit bytes",
        "int8": "8 bit integer",
        "int16": "16 bit integer",
        "int32": "32 bit integer",
        "int64": "64 bit integer",
        "uint8": "8 bit unsigned integer",
        "uint16": "16 bit unsigned integer",
        "uint32": "32 bit unsigned integer",
        "uint64": "64 bit unsigned integer",
        "integer": "32 bit integer",
        "boolean": "boolean",
        "decimal": "Decimal",
        "float": "32 bit float",
        "double": "64 bit float",
        "date": "datetime.date",
        "datetime": "datetime.datetime",
        "time": "datetime.time",
    }

    def __init__(self, schema_dict):
        self._fields = {
            k: FieldSchema.model_validate(v)
            for k, v in schema_dict.items()
        }
        self._schema = schema_dict

    @property
    def schema(self):
        """Raw field schema dict."""
        return self._schema

    def validate(self, row):
        """Check that all row keys are known schema fields.

        Args:
            row: dict of field name to value

        Returns:
            True if all keys are present in schema, False otherwise
        """
        return all(k in self._fields for k in row)

    @staticmethod
    def dict_from_iterable(the_iterable, infer_strings: bool = False,
                           strict_numbers=False, p=1.0, stop=100):
        """
        Infer field schema dict from iterable.

        Args:
            the_iterable: the data
            infer_strings: attempt to infer data types of string values
                (e.g. dates or numbers)
            strict_numbers: remove commas from numbers when false
            p: probability of evaluating a record
            stop: stop after n rows

        Returns:
            OrderedDict mapping field names to field property dicts
        """
        sniffer = infer.InferSchema(
            infer_strings=infer_strings,
            strict_numbers=strict_numbers,
            p=p,
            stop=stop
        )
        sniffer(the_iterable)
        dict_schema = collections.OrderedDict()
        for key, stats in sniffer.summary.items():
            node = {}
            node["type"] = stats.str_type
            if stats.type == str:
                node["str_len"] = stats.max
            dict_schema[key] = node
        return dict_schema

    @classmethod
    def from_iterable(cls, the_iterable, strict=False):
        """
        Infer schema from iterable and return an EntityValidator instance.

        Args:
            the_iterable: source data
            strict: passed to dict_from_iterable as infer_strings

        Returns:
            EntityValidator instance
        """
        return cls(cls.dict_from_iterable(the_iterable, strict))
