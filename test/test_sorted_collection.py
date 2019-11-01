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
from dkit.data.containers import SortedCollection
import unittest
from random import shuffle


class TestSortedCollection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.n = 100
        cls.lst = (list(range(cls.n)))
        shuffle(cls.lst)
        cls.c = SortedCollection(cls.lst)

    def test_len(self):
        self.assertEqual(len(self.c), self.n)

    def test_min(self):
        self.assertEqual(self.c.min_value, 0)

    def test_max(self):
        self.assertEqual(self.c.max_value, self.n-1)

    def test_contains(self):
        self.assertEqual(0 in self.c, True)
        self.assertEqual(self.n-1 in self.c, True)

    def test_count(self):
        self.assertEqual(self.c.count(0), 1)

    def test_find_functions(self):
        self.assertEqual(self.c.find_gt(10), 11)
        self.assertEqual(self.c.find_ge(10), 10)
        self.assertEqual(self.c.find_lt(10), 9)
        self.assertEqual(self.c.find_le(10), 10)

    def test_index(self):
        self.assertEqual(self.c.index(0), 0)
        self.assertEqual(self.c.index(10), 10)


if __name__ == "__main__":
    unittest.main()
