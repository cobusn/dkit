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
Classes and utilities for manage ment of data model schemas

This module relies and extends the Cerberus python library
"""

import collections

import cerberus
from dkit.data import infer
from dateutil import parser
import decimal


class ModelFactory(object):

    def __init__(self, default_str_len=255):
        self.default_str_len = default_str_len


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
        return bool(parser.parse(value))
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


decimal_type = cerberus.TypeDefinition('decimal', (decimal.Decimal,), ())


class EntityValidator(cerberus.Validator):
    """
    Custom Cerberus Schema Validator

    Additional defined properties are:
        - str_len
        - primary_key
        - index

    """
    # map model types to python types
    types_mapping = cerberus.Validator.types_mapping.copy()
    types_mapping['decimal'] = decimal_type
    map_python = {
        "boolean": parse_bool,
        "integer": parse_int,
        "float": parse_float,
        "string": str,
        "datetime": parse_datetime,
        "date": parse_datetime,
        "decimal": parse_decimal,
    }

    def _validate_str_len(self, strlen, field, value):
        """
         {'type': 'integer'}
        """
        if not isinstance(strlen, int):
            self._error(field, "Must be integer value")

    def _validate_primary_key(self, primarykey, field, value):
        """
         {'type': 'boolean'}
        """
        if primarykey and not isinstance(primarykey, bool):
            self._error(field, "Must be boolean.")

    def _validate_type_int8(self, field, value):
        if not isinstance(value, int):
            self._error(field, "Is not an integer instance")

    def _validate_type_int16(self, field, value):
        if not isinstance(value, int):
            self._error(field, "Is not an integer instance")

    def _validate_type_int32(self, field, value):
        if not isinstance(value, int):
            self._error(field, "Is not an integer instance")

    def _validate_type_int64(self, field, value):
        if not isinstance(value, int):
            self._error(field, "Is not an integer instance")

    def _validate_type_double(self, field, value):
        if not isinstance(value, float):
            self._error(field, "Is not a floating point instance")

    def _validate_unique(self, _unique, field, value):
        """
         {'type': 'boolean'}
        """
        if _unique and not isinstance(_unique, bool):
            self._error(field, "Must be boolean.")

    def _validate_index(self, _index, field, value):
        """
         {'type': 'boolean'}
        """
        if _index and not isinstance(_index, bool):
            self._error(field, "Must be boolean.")

    @staticmethod
    def dict_from_iterable(the_iterable, strict=False, p=1.0, stop=100):
        """
        infer dict_schema from iterable

        Args:
            strict: remove commas from numbers when false
            p: probability of evaluating a record
            stop: stop after n rows
        """
        sniffer = infer.InferTypes(strict)
        sniffer(the_iterable, strict, p=p, stop=stop)
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
        infer dict_schema from iterable
        """
        return cls(cls.dict_from_iterable(the_iterable, strict))
