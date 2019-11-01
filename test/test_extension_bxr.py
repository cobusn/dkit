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
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
import os
import unittest
import sys
sys.path.insert(0, "..")

from dkit.etl.reader import FileReader, Bz2Reader
from dkit.etl.source import CsvDictSource
from dkit.etl.writer import FileWriter, Bz2Writer
from dkit.etl.writer import CodecWriter
from dkit.etl.extensions.ext_bxr import BXRSink, BXRSource
from create_data import FIELD_NAMES


class TestBXRSink(unittest.TestCase):

    def setUp(self):
        self.csv_source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])

    def test_defaults(self):
        """Test writing with defaults"""
        writer = FileWriter(os.path.join("output", "bxr_dict_writer_output.bxr"))
        BXRSink(writer).process(self.csv_source)

    def test_field_names(self):
        """Test writing with different delimiter"""
        # ip,company,year,id,name
        writer = FileWriter(os.path.join("output", "jsonl_dict_writer_field_names.jsonl"))
        BXRSink(writer, field_names=['ip', 'ip', 'name']).process(self.csv_source)

    def test_codec(self):
        path = os.path.join("output", "bxr_dict_writer_utf_8.bxr")
        writer = CodecWriter(path, codec="utf-8")
        BXRSink(writer, field_names=['ip', 'ip', 'name']).process(self.csv_source)

    def test_bzip2(self):
        """
        Need to fix utf-8 with bzip2 and python 2.
        """
        path = os.path.join("output", "bxr_dict_writer_bz2.bxr.bz2")
        BXRSink(Bz2Writer(path)).process(self.csv_source)


class TestBXRSource(unittest.TestCase):

    def setUp(self):
        self.source = BXRSource([FileReader(os.path.join("input_files", "sample.bxr"))])
        self.list = [i for i in self.source]

    def test_reset(self):
        """
        test reset() method
        """
        l1 = list(self.source)
        self.source.reset()
        l2 = list(self.source)
        self.assertEqual(l1, l2)

    def test_int(self):
        """
        Test that integer return correctly
        """
        val = self.list[0]["year"]
        self.assertEqual(isinstance(val, int), True)

    def test_float(self):
        """
        test that float return correctly
        """
        val = self.list[0]["score"]
        self.assertEqual(isinstance(val, float), True)

    def test_from_bzip(self):
        """
        test that bzip2 work correctly
        """
        source = BXRSource([Bz2Reader(os.path.join("input_files", "sample.bxr.bz2"))])
        the_list = [i for i in source]
        self.assertEqual(len(the_list), 500)

    def test_field_names(self):
        """
        Test that all columns are available.
        """
        first = self.list[0]
        for i in FIELD_NAMES:
            self.assertEqual(i in first.keys(), True)
        self.assertEqual(len(first.keys()), len(FIELD_NAMES))


if __name__ == '__main__':
    unittest.main()
