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

from dkit.etl.reader import FileReader
from dkit.etl.source import CsvDictSource
from dkit.etl.writer import FileWriter, Bz2Writer
from dkit.etl.writer import CodecWriter
from dkit.etl.sink import JsonlSink


class TestJsonlSink(unittest.TestCase):

    def setUp(self):
        self.csv_source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])

    def test_defaults(self):
        """Test writing with defaults"""
        writer = FileWriter(os.path.join("output", "jsonl_dict_writer_output.jsonl"))
        JsonlSink(writer).process(self.csv_source)

    def test_bzip2(self):
        """
        Need to fix utf-8 with bzip2 and python 2.
        """
        path = os.path.join("output", "jsonl_dict_writer_bz2.jsonl.bz2")
        JsonlSink(Bz2Writer(path)).process(self.csv_source)

    def __test_bzip2_utf8(self):
        """
        Need to fix utf-8 with bzip2 and python 2.
        """
        path = os.path.join("output", "jsonl_dict_writer_utf_8.jsonl.bz2")
        writer = Bz2Writer(path, codec="utf-8")
        JsonlSink(writer, field_names=['ip', 'ip', 'name']).process(self.csv_source)


if __name__ == '__main__':
    unittest.main()
