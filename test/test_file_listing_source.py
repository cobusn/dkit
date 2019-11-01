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
import sys
import unittest
sys.path.insert(0, "..")

from dkit.etl.source import FileListingSource


class TestCase(unittest.TestCase):

    def setUp(self):
        self.o = FileListingSource(["input_files/*bz2", "input_files/*.csv"])

    def test_csv_in_list(self):
        """
        Test that csv files in list
        """
        self.assertEqual(True, any(["csv" in i for i in list(self.o)]))

    def test_bz2_in_list(self):
        """
        Test that bz2 files are in list
        """
        self.assertEqual(True, any([".bz2" in i for i in list(self.o)]))


if __name__ == '__main__':
    unittest.main()
