#
# Copyright (C) 2014  Cobus Nel
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
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

import sys; sys.path.insert(0, "..")   # noqa
import unittest
import datetime

from dkit.data.infer import infer_type, InferSchema, ExtractSchemaInline
from dkit.etl import source
import common


class TestInferType(common.TestBase):
    """Test the Timer class"""

    def test_bool(self):
        """
        test infer bool types
        """
        tests = [True, False, "True", "False", "true", "no", "yes"]
        for test in tests:
            t = infer_type(test)
            self.assertEqual(t, bool)

    def test_int(self):
        """
        test infer int types
        """
        tests = [1, '12', '34', '-3', '3', ' 39 ']
        for test in tests:
            t = infer_type(test, strict=True)
            self.assertEqual(t, int)

        # using strict
        tests = [1, '12', '34', '-3', '3', ' 300 ', '300,0']
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, int)

    def test_float(self):
        """
        test infer float types
        """
        tests = [1.0, '-0.00001', '12.1', '34.1', '-3.5', '3E5', ' 300.0 ', ' 3,00.0 ', '300,0.4E4']
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, float)

    def test_str(self):
        """
        test infer str typs
        """
        tests = ["asdf", r"a@#%@", "a 2342", "A,2342", "1233ss"]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, str)

    def test_date(self):
        """
        test infer date types
        """
        tests = ["1 jan 2010", "5/5/2015", "5/5/05", "5-5-2015", "3 December 2016"]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, datetime.date)

    def test_datetime(self):
        """
        test infer datetime types
        """
        tests = [
            "2010-01-05 12:13:14",
            "2010-01-05T12:13:14",
            "12:00",
        ]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, datetime.datetime)

    def test_date_like_false_positives_remain_strings(self):
        """
        test date detection does not consume general text
        """
        tests = [
            "report 2024 final",
            "model 3",
            "1233ss",
            "Janus",
            "batch-2024-a",
            "Q1 2024",
        ]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, str)

    def test_invalid_date_like_strings_remain_strings(self):
        """
        test invalid date like values are not inferred as datetime
        """
        tests = [
            "2024-13-01",
            "2024-02-30",
            "2024-99-99 10:00:00",
        ]
        for test in tests:
            t = infer_type(test, strict=False)
            self.assertEqual(t, str)


class TestInferTypes(common.TestBase):
    """Test the Timer class"""

    def test_1(self):
        data = [
            {"_str": "Str", "_int": "10", "_float": "10.2", "_datetime": "5 Jan 2016"},
            {"_str": "String", "_float": "100.2", "_datetime": "5 February 2017"},
        ]
        checker = InferSchema()
        checker(data)
        for row in checker.summary.values():
            print(row)


mtcars_schema = {
    'car': str,
    'mpg': float,
    'cyl': int,
    'disp': float,
    'hp': int,
    'drat': float,
    'wt': float,
    'qsec': float,
    'vs': int,
    'am': int,
    'gear': int,
    'carb': int
}


class TestExtractTypes(unittest.TestCase):

    def test_1(self):
        with source.load("data/mtcars.csv") as src:
            data_before = list(src)
            e = ExtractSchemaInline(src)
            print(e.schema)
            self.assertEqual(len(data_before), len(list(e)))
            self.assertEqual(e.schema, mtcars_schema)


if __name__ == '__main__':
    unittest.main()
