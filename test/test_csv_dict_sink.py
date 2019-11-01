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

from dkit.utilities import log_helper as log
from dkit.etl.reader import FileReader
from dkit.etl.source import CsvDictSource
from dkit.etl.writer import FileWriter
from dkit.etl.sink import CsvDictSink


class TestKeyIndexer(unittest.TestCase):

    def setUp(self):
        self.csv_source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])

    def test_defaults(self):
        """Test writing with defaults"""
        writer = FileWriter(os.path.join("output", "csv_dict_writer_output.csv"))
        CsvDictSink(writer).process(self.csv_source)

    def test_terminator(self):
        """Test writing with different line terminator"""
        writer = FileWriter(os.path.join("output", "csv_dict_writer_terminator.csv"))
        CsvDictSink(writer, lineterminator='\n\n').process(self.csv_source)

    def test_delimiter(self):
        """Test writing with different delimiter"""
        writer = FileWriter(os.path.join("output", "csv_dict_writer_delimiter.csv"))
        CsvDictSink(writer, delimiter='\t').process(self.csv_source)

    def test_no_headings(self):
        """Test writing with different delimiter"""
        writer = FileWriter(os.path.join("output", "csv_dict_writer_noheadings.csv"))
        CsvDictSink(writer, write_headings=False).process(self.csv_source)

    def test_field_names(self):
        """Test writing with different delimiter"""
        # ip,company,year,id,name
        writer = FileWriter(os.path.join("output", "csv_dict_writer_field_names.csv"))
        CsvDictSink(writer, field_names=["name", "id", "id"]).process(self.csv_source)

    def test_csv_writer_stats(self):
        logger = log.file_logger("output/write_stats.log", "csv stats")
        gen = ({"key": i} for i in range(1000))
        writer = FileWriter("output/stats.csv")
        sink = CsvDictSink(writer, logger=logger)
        sink.stats.trigger = 50
        sink.process(gen)


if __name__ == '__main__':
    unittest.main()
