# Copyright (c) 2019 Cobus Nel
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
danipulation routines.

=========== =============== =================================================
Jul 2019    Cobus Nel       Created
=========== =============== =================================================
"""
import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.etl import source
from dkit.etl.extensions.ext_parquet import ParquetSink, ParquetSource
# from dkit.etl.extensions.ext_arrow import OSFileWriter

PARQUET_FILE = "output/test.parquet"


class TestAParquetSink(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with source.load("output/speed.jsonl") as input_data:
            cls.data = list(input_data)

    def test_write_all_fields(self):
        sink = ParquetSink(PARQUET_FILE, chunk_size=1000)
        sink.process(self.data)


class TestBParquetSource(unittest.TestCase):

    def test_read_all_fields(self):
        rows = list(ParquetSource(PARQUET_FILE))
        print(len(rows))


if __name__ == '__main__':
    unittest.main()
