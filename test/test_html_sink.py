#
# Copyright (C) 2018  Cobus Nel
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
from dkit.etl.sink import HtmlTableSink


class TestHtmlTableSink(unittest.TestCase):

    def setUp(self):
        self.data_source = CsvDictSource(
            [FileReader(os.path.join("input_files", "sample.csv"))]
        )

    def test_defaults(self):
        """Test writing with defaults"""
        writer = FileWriter(os.path.join("output", "html_dict_writer_output.html"))
        HtmlTableSink(writer).process(self.data_source)

    def test_field_names(self):
        """Test writing with different delimiter"""
        # ip,company,year,id,name
        writer = FileWriter(os.path.join("output", "html_dict_writer_field_names.html"))
        HtmlTableSink(writer, field_names=['ip', 'ip', 'name']).process(self.data_source)

    def test_codec(self):
        path = os.path.join("output", "html_dict_writer_utf_8.html")
        writer = CodecWriter(path, codec="utf-8")
        HtmlTableSink(writer, field_names=['ip', 'ip', 'name']).process(self.data_source)

    def test_bzip2(self):
        """
        Need to fix utf-8 with bzip2 and python 2.
        """
        path = os.path.join("output", "html_dict_writer_bz2.html.bz2")
        HtmlTableSink(Bz2Writer(path)).process(self.data_source)

    def __test_bzip2_utf8(self):
        """
        Need to fix utf-8 with bzip2 and python 2.
        """
        path = os.path.join("output", "html_dict_writer_utf_8.html.bz2")
        writer = Bz2Writer(path, codec="utf-8")
        HtmlTableSink(writer, field_names=['ip', 'ip', 'name']).process(self.data_source)


if __name__ == '__main__':
    unittest.main()
