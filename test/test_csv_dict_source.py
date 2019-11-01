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
from __future__ import print_function
import os
import unittest
import sys
sys.path.insert(0, "..")

from dkit.etl.reader import FileReader
from dkit.etl.reader import Bz2Reader
from dkit.etl.source import CsvDictSource
from dkit.utilities import log_helper as log
from create_data import FIELD_NAMES


class TestCsvDictSource(unittest.TestCase):

    def setUp(self):
        self.source = CsvDictSource([FileReader(os.path.join("input_files", "sample.csv"))])
        self.list = [i for i in self.source]

    def test_reset(self):
        """
        test reset() method
        """
        l1 = list(self.source)
        self.source.reset()
        l2 = list(self.source)
        self.assertEqual(l1, l2)

    def test_len(self):
        self.assertEqual(len(self.list), 500)

    def test_from_bzip(self):
        """
        test that bzip2 work correctly
        """
        source = CsvDictSource([Bz2Reader(os.path.join("input_files", "sample.csv.bz2"))])
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

    def test_csv_reader_stats(self):
        r = FileReader("input_files/sample.csv")
        logger = log.file_logger("output/read_stats.log", "csv read stats")
        src = CsvDictSource([r], logger=logger)
        src.stats.trigger = 20
        _ = list(src)

if __name__ == '__main__':
    unittest.main()
