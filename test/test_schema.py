#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
import sys
sys.path.insert(0, "..")
import datetime
import decimal
import unittest

from dkit.etl import schema, source, reader, transform


SAMPLE_FIELD_DICTS = {
    "string":   {"type": "string", "str_len": 20},
    "integer":  {"type": "integer"},
    "float":    {"type": "float"},
    "boolean":  {"type": "boolean"},
    "decimal":  {"type": "decimal", "scale": 2, "precision": 10},
    "datetime": {"type": "datetime"},
    "date":     {"type": "date"},
    "binary":   {"type": "binary"},
    "int8":     {"type": "int8"},
    "int16":    {"type": "int16"},
    "int32":    {"type": "int32"},
    "int64":    {"type": "int64"},
    "uint8":    {"type": "uint8"},
    "uint16":   {"type": "uint16"},
    "uint32":   {"type": "uint32"},
    "uint64":   {"type": "uint64"},
    "double":   {"type": "double"},
}

COERCE_INPUT = [
    {
        "name": "Alice",
        "age": "42",
        "score": "3.14",
        "active": "True",
        "amount": "9.99",
        "created": "2024-01-15",
    }
]

COERCE_SCHEMA = {
    "name":    {"type": "string"},
    "age":     {"type": "integer"},
    "score":   {"type": "float"},
    "active":  {"type": "boolean"},
    "amount":  {"type": "decimal"},
    "created": {"type": "datetime"},
}


class TestEntityValidatorFieldTypes(unittest.TestCase):
    """Each supported type name must be accepted without validation errors."""

    def _make_validator(self, field_dict):
        return schema.EntityValidator({"field": field_dict})

    def test_string_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["string"])
        self.assertIsNotNone(v)

    def test_integer_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["integer"])
        self.assertIsNotNone(v)

    def test_float_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["float"])
        self.assertIsNotNone(v)

    def test_boolean_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["boolean"])
        self.assertIsNotNone(v)

    def test_decimal_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["decimal"])
        self.assertIsNotNone(v)

    def test_datetime_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["datetime"])
        self.assertIsNotNone(v)

    def test_date_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["date"])
        self.assertIsNotNone(v)

    def test_binary_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["binary"])
        self.assertIsNotNone(v)

    def test_int8_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["int8"])
        self.assertIsNotNone(v)

    def test_int64_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["int64"])
        self.assertIsNotNone(v)

    def test_uint16_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["uint16"])
        self.assertIsNotNone(v)

    def test_double_field(self):
        v = self._make_validator(SAMPLE_FIELD_DICTS["double"])
        self.assertIsNotNone(v)


class TestEntityValidatorProperties(unittest.TestCase):
    """Extra field properties must be accepted."""

    def test_str_len_accepted(self):
        v = schema.EntityValidator({"f": {"type": "string", "str_len": 50}})
        self.assertIsNotNone(v)

    def test_scale_and_precision_accepted(self):
        v = schema.EntityValidator(
            {"f": {"type": "decimal", "scale": 4, "precision": 12}}
        )
        self.assertIsNotNone(v)

    def test_primary_key_accepted(self):
        v = schema.EntityValidator(
            {"f": {"type": "integer", "primary_key": True}}
        )
        self.assertIsNotNone(v)

    def test_unique_accepted(self):
        v = schema.EntityValidator({"f": {"type": "string", "unique": True}})
        self.assertIsNotNone(v)

    def test_index_accepted(self):
        v = schema.EntityValidator({"f": {"type": "integer", "index": True}})
        self.assertIsNotNone(v)

    def test_computed_accepted(self):
        v = schema.EntityValidator(
            {"f": {"type": "string", "computed": True}}
        )
        self.assertIsNotNone(v)


class TestDictFromIterable(unittest.TestCase):
    """dict_from_iterable must infer field types from data."""

    ROWS = [
        {"name": "Alice", "age": 30, "score": 1.5},
        {"name": "Bob",   "age": 25, "score": 2.0},
    ]

    def test_returns_all_fields(self):
        d = schema.EntityValidator.dict_from_iterable(self.ROWS)
        self.assertIn("name", d)
        self.assertIn("age", d)
        self.assertIn("score", d)

    def test_each_field_has_type_key(self):
        d = schema.EntityValidator.dict_from_iterable(self.ROWS)
        for field, spec in d.items():
            self.assertIn("type", spec, f"field '{field}' missing 'type' key")

    def test_string_field_has_str_len(self):
        d = schema.EntityValidator.dict_from_iterable(self.ROWS)
        self.assertIn("str_len", d["name"])

    def test_numeric_types_inferred(self):
        d = schema.EntityValidator.dict_from_iterable(self.ROWS)
        self.assertIn(d["age"]["type"], ("integer", "int32", "int64"))
        self.assertIn(d["score"]["type"], ("float", "double"))


class TestMapPythonCompleteness(unittest.TestCase):
    """map_python must cover the coercible types listed in type_description."""

    COERCIBLE = {"boolean", "integer", "float", "string", "datetime",
                 "date", "decimal", "binary"}

    def test_coercible_types_present(self):
        for t in self.COERCIBLE:
            self.assertIn(
                t, schema.EntityValidator.map_python,
                f"type '{t}' missing from map_python"
            )

    def test_map_python_values_are_callable(self):
        for t, fn in schema.EntityValidator.map_python.items():
            self.assertTrue(callable(fn), f"map_python['{t}'] is not callable")


class TestCoerceTransformTypes(unittest.TestCase):
    """CoerceTransform must produce correctly typed values."""

    @classmethod
    def setUpClass(cls):
        validator = schema.EntityValidator(COERCE_SCHEMA)
        cls.result = list(
            transform.CoerceTransform(validator)(COERCE_INPUT)
        )

    def test_one_row_returned(self):
        self.assertEqual(len(self.result), 1)

    def test_string_type(self):
        self.assertIsInstance(self.result[0]["name"], str)

    def test_integer_type(self):
        self.assertIsInstance(self.result[0]["age"], int)

    def test_float_type(self):
        self.assertIsInstance(self.result[0]["score"], float)

    def test_boolean_type(self):
        self.assertIsInstance(self.result[0]["active"], bool)

    def test_decimal_type(self):
        self.assertIsInstance(self.result[0]["amount"], decimal.Decimal)

    def test_datetime_type(self):
        self.assertIsInstance(self.result[0]["created"], datetime.datetime)


class TestCoerceTransformMissingField(unittest.TestCase):
    """CoerceTransform must handle missing keys gracefully."""

    def test_missing_field_yields_none(self):
        validator = schema.EntityValidator({"age": {"type": "integer"}})
        result = list(
            transform.CoerceTransform(validator)([{}])
        )
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["age"])


class TestFromIterableViaFile(unittest.TestCase):
    """EntityValidator.from_iterable must build a validator from a JSONL file."""

    @classmethod
    def setUpClass(cls):
        _src = source.JsonlSource([reader.FileReader("input_files/slope.jsonl")])
        cls.validator = schema.EntityValidator.from_iterable(_src)

    def test_validator_has_schema(self):
        self.assertTrue(len(self.validator.schema) > 0)

    def test_all_fields_have_type(self):
        for field, spec in self.validator.schema.items():
            self.assertIn("type", spec, f"field '{field}' missing 'type'")

    def test_coerce_runs_without_error(self):
        src = source.JsonlSource([reader.FileReader("input_files/slope.jsonl")])
        result = list(transform.CoerceTransform(self.validator)(src))
        self.assertTrue(len(result) > 0)


if __name__ == '__main__':
    unittest.main()
