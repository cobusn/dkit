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
import sys; sys.path.insert(0, "..")

from dkit.etl.reader import FileReader
from dkit.etl.source import CsvDictSource
from dkit.etl.extensions.ext_msgpack import MsgpackSink
from dkit.etl.writer import FileWriter, Bz2Writer


class TestMsgpackSink(unittest.TestCase):

    def setUp(self):
        self.csv_source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])

    def test_defaults(self):
        """Test writing with defaults"""
        writer = FileWriter(os.path.join("output", "msgpack_output.mpak"), mode="wb")
        MsgpackSink(writer).process(self.csv_source)

    def test_field_names(self):
        """Test writing with different delimiter"""
        # ip,company,year,id,name
        writer = FileWriter(os.path.join("output", "msgpack_field_names.mpak"), mode="wb")
        MsgpackSink(writer, field_names=['ip', 'ip', 'name']).process(self.csv_source)

    def test_bzip2(self):
        path = os.path.join("output", "msgpack_writer.mpak.bz2")
        MsgpackSink(Bz2Writer(path, mode="wb")).process(self.csv_source)


if __name__ == '__main__':
    unittest.main()
