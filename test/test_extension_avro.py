# Copyright (c) 2022 Cobus Nel
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

import unittest
import sys;  sys.path.insert(0, "..") # noqa
from datetime import datetime, date, timezone
from decimal import Decimal

from dkit.etl.extensions.ext_avro import AvroSink, AvroSource
from dkit.etl.writer import FileWriter
from dkit.etl.reader import FileReader
from dkit.etl import source, sink
from dkit.etl.model import ModelManager
# from dkit.data.console import head

AVRO_FILE = "output/mtcars.avro"
with source.load("data/mtcars.jsonl") as infile:
    MTCARS = list(infile)


def gen_logical_type_data():
    """generate data for logical types"""
    rv = []
    for i in range(100):
        rv.append(
            {
                "_date": date.today(),
                "_datetime": datetime.now(timezone.utc),
                "_decimal": Decimal(10),
            }
        )
    return rv


class AvroTest(unittest.TestCase):

    def _assert_file(self, compare, compare_list):
        """assert that compare_to is the same as the original"""
        for i, row in enumerate(compare):
            self.assertEqual(row, compare_list[i])


class A_TestAvroSink(AvroTest):

    def test_extended_types(self):
        """test type compatibility"""
        data = gen_logical_type_data()
        with sink.load("data/compat.avro") as snk:
            snk.process(data)
        with source.load("data/compat.avro") as src:
            for i, record in enumerate(src):
                for f in record:
                    self.assertTrue(
                        record[f] == data[i][f]
                    )

    def test_provided_schema(self):
        """test avro writer with provided schema"""
        m = ModelManager.from_file("model.yml")
        e = m.entities["mtcars"]
        w = FileWriter("data/mtcars1.avro", "wb")
        snk = AvroSink(w, schema=e)
        snk.process(MTCARS)
        with source.load("data/mtcars1.avro") as src:
            self._assert_file(src, MTCARS)

    def test_avro_sink_auto_schema(self):
        """test auto generated schema"""
        w = FileWriter(AVRO_FILE, "wb")
        snk = AvroSink(w)
        snk.process(MTCARS)

    def test_avro_sink_auto(self):
        """test auto generated schema"""
        with sink.load("data/mtcars1.avro") as snk:
            snk.process(MTCARS)
        with source.load("data/mtcars1.avro") as src:
            self._assert_file(src, MTCARS)


class B_TestAvroSource(AvroTest):

    def test_avro_source(self):
        """test auto generated schema"""
        r = FileReader(AVRO_FILE, "rb")
        self._assert_file(AvroSource([r]), MTCARS)

    def test_avro_source_autoload(self):
        """
        with source
        """
        with source.load(AVRO_FILE) as src:
            self._assert_file(src, MTCARS)

    def test_avro_source_some_fields(self):
        """
        test the iter_field_names function
        """
        fields = ["am", "carb", "disp"]
        r = FileReader(AVRO_FILE, "rb")
        out = AvroSource([r], field_names=fields)
        for i, record in enumerate(out):
            a = {k: MTCARS[i][k] for k in fields}
            self.assertEqual(a, record)


if __name__ == '__main__':
    unittest.main()
