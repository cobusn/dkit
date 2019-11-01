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
import common
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
sys.path.insert(0, "..") # noqa
from dkit.utilities import intervals


class TestTimeHelper(common.TestBase):
    """Test the intervals module"""

    def test_tomorrow(self):
        """
        tomorrow()
        """
        tomorrow = date.today() + timedelta(days=1)
        self.assertEqual(intervals.tomorrow()[0], tomorrow)

    def test_today(self):
        """
        today()
        """
        self.assertEqual(intervals.today()[0], date.today())

    def test_next_week(self):
        """
        next_week()
        """
        today = date.today()
        mon_next_week = date.today() + timedelta(days=7-today.weekday())
        self.assertEqual(intervals.next_week()[0], mon_next_week)

    def test_next_month(self):
        """
        next_month()
        """
        today = date.today()
        first_this_month = date(today.year, today.month, 1)
        first_next_month = first_this_month + relativedelta(first_this_month, months=1)
        self.assertEqual(intervals.next_month()[0], first_next_month)

    def test_yesterday(self):
        """
        tomorrow()
        """
        yesterday = date.today() - timedelta(days=1)
        self.assertEqual(intervals.yesterday()[0], yesterday)


if __name__ == '__main__':
    unittest.main()
