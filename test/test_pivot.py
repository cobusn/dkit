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

import sys
import unittest
sys.path.insert(0, "..") # noqa
from random import choice
from random import randrange

from dkit.data import manipulate
from dkit.utilities import instrumentation
import common


class TestPivot(common.TestBase):
    """Test the Timer class"""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.data = [
            {"year": 1999, "name": "John", "id": 1, "points": 10},
            {"year": 1998, "name": "Andy", "id": 2, "points": 20},
            {"year": 1999, "name": "John", "id": 1, "points": 15},
            {"year": 1997, "name": "Susan", "id": 3, "points": 16},
        ]

    def setUp(self):
        self.pivot = manipulate.Pivot(self.data, ["id", "name"], "year", "points", sum)

#     def test_iter_order(self):
#         """Test that row ordered correctly."""
#         p = list(self.pivot)
#         order = (p[0]["id"] == 1) & (p[1]["id"] == 2) & (p[2]["id"] == 3)
#         self.assertEqual(order, True)

    def test_col_order(self):
        """Test column name order"""
        self.assertEqual(["id", "name", "1997", "1998", "1999"], self.pivot.column_headings)

#    def test_col_order_reversed(self):
#        """Test for column headings in reverse order"""
#        p = manipulate.Pivot(self.data, ["id", "name"], "year", "points", sum, reverse_cols=True)
#         self.assertEqual(["id", "name", '1999', '1998', '1997'], p.column_headings)

    def test_function_constructor(self):
        """Test specification of different function in constructor"""
        pivot = manipulate.Pivot(self.data, ["id", "name"], "year", "points", function=min)
        list_p = sorted(list(pivot), key=lambda x: x["id"])
        self.assertEqual(list_p[0]["1999"], 10)

#     def test_row_order(self):
#         """Test row order d"""
#         p = manipulate.Pivot(self.data, ["id", "name"], "year", "points", sum)
#         self.assertEqual(list(p)[2]["id"], 1)
#         self.assertEqual(list(p)[0]["id"], 3)

    def test_missing_constructor(self):
        """Test different missing value in constructor"""
        p = manipulate.Pivot(self.data, ["id", "name"], "year", "points", sum, missing=1)
        list_p = sorted(list(p), key=lambda x: x["id"])
        self.assertEqual(list_p[0]["1998"], 1)

    def test_rows(self):
        """Test using rows function with different parameters"""
        p = list(self.pivot.rows(function=min, missing=1))
        list_p = sorted(list(p), key=lambda x: x["id"])
        self.assertEqual(list_p[0]["1998"], 1)
        self.assertEqual(list_p[0]["1999"], 10)

    def test_xperformance(self):
        timer = instrumentation.CounterLogger()
        n = 100000
        years = ["1997", "1998", "1999"]
        names = ["John", "Andy", "Susan"]
        the_data = [
            {
                "name": choice(names),
                "year": choice(years),
                "score": randrange(0, 100)
            } for i in range(n)
        ]

        timer.start()
        _ = manipulate.Pivot(the_data, ["name"], "year", "score", sum)
        timer.stop()

        print("\nPivoted {} items in {} seconds at {} items/second.\n".format(
            n, timer.seconds_elapsed, n/timer.seconds_elapsed)
        )


if __name__ == '__main__':
    unittest.main()
