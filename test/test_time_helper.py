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
from datetime import date, timedelta, datetime, timezone
from dateutil.relativedelta import relativedelta
sys.path.insert(0, "..") # noqa
from dkit.utilities import time_helper, intervals


class TestTimeHelper(common.TestBase):
    """Test the time_helper module"""

    def test_hms_function(self):
        """
        test hms function
        """
        hours = 2
        minutes = 4
        seconds = 44
        milliseconds = 0.44
        val = (hours * 60 * 60) + (minutes * 60) + seconds + milliseconds
        h, m, s, ms = time_helper.hms(val)
        self.assertEqual(h, hours)
        self.assertEqual(minutes, m)
        self.assertEqual(milliseconds, ms / 1000.0)
        self.assertEqual(seconds, s)

    def test_from_unixtime(self):
        """
        to_unixtime
        """
        tz_za = timezone(timedelta(hours=2))
        dt = datetime(2017, 1, 1, tzinfo=tz_za)
        unixtime = time_helper.to_unixtime(dt)
        self.assertEqual(unixtime, 1483221600)

    def test_to_unixtime(self):
        """
        to_unixtime()
        """
        tz_za = timezone(timedelta(hours=2))
        dt = datetime(2017, 1, 1, tzinfo=tz_za)
        unixtime = time_helper.from_unixtime(1483221600, tz_za)
        self.assertEqual(unixtime, dt)

    def test_date_range(self):
        """
        daterange()
        """
        drange = list(time_helper.daterange(
            date(2019, 1, 1),
            date(2020, 1, 1),
            relativedelta(months=1)
        ))
        self.assertEqual(len(drange), 12)
        self.assertEqual(
            drange[-1],
            date(2019, 12, 1)
        )

    def test_date_range_hour(self):
        """
        daterange()
        """
        drange = list(time_helper.daterange(
            datetime(2020, 1, 1),
            datetime(2020, 1, 2),
            relativedelta(hours=1)
        ))
        self.assertEqual(len(drange), 24)
        self.assertEqual(
            drange[-1],
            datetime(2020, 1, 1, 23, 0)
        )

    def test_last_day_of_month(self):
        tests = [
            (date(2022, 1, 4), date(2022, 1, 31)),
            (datetime(2022, 1, 4), date(2022, 1, 31)),
            (datetime(2024, 2, 1), date(2024, 2, 29)),
        ]
        for test in tests:
            self.assertEqual(
                time_helper.last_day_of_month(test[0]),
                test[1]
            )

    def test_date_range_pairs(self):
        """
        daterange()
        """
        drange = list(time_helper.daterange_pairs(
            date(2019, 1, 1),
            date(2020, 1, 1),
            relativedelta(months=1)
        ))
        self.assertEqual(len(drange), 12)
        self.assertEqual(
            drange[-1],
            (date(2019, 12, 1), date(2020, 1, 1))
        )

    def test_month_day_id(self):
        """
        month_day_id
        """
        m_id = 1672524000
        d_id = 1672610400
        tz = time_helper.get_tz(2)
        d1 = datetime(2023, 1, 2, tzinfo=tz)

        m1, d1 = time_helper.month_day_id(d1)
        self.assertEqual(m1, m_id)
        self.assertEqual(d1, d_id)

    def test_month_day_id_2(self):
        """
        month_day_id_2
        """
        m_id = 1672524000
        d_id = 1672610400
        ts = d_id + 200
        tz = time_helper.get_tz(2)
        m1, d1 = time_helper.month_day_id_2(ts, tz)
        self.assertEqual(m1, m_id)
        self.assertEqual(d1, d_id)

    def test_short_month_day_id(self):
        """
        short_month_day_id
        """
        m_id = 20230101
        d_id = 20230102
        tz = time_helper.get_tz(2)
        d1 = datetime(2023, 1, 2, tzinfo=tz)

        m1, d1 = time_helper.short_month_day_id(d1)
        self.assertEqual(m1, m_id)
        self.assertEqual(d1, d_id)

    def test_short_month_day_id_2(self):
        """
        short_month_day_id_2
        """
        m_id = 20230101
        d_id = 20230102
        tz = time_helper.get_tz(2)
        d1 = datetime(2023, 1, 2, tzinfo=tz)
        ts = time_helper.to_unixtime(d1) + 200
        m1, d1 = time_helper.short_month_day_id_2(ts, tz)
        self.assertEqual(m1, m_id)
        self.assertEqual(d1, d_id)

    def test_parse_keyword(self):
        yesterday = datetime.combine(
            intervals.yesterday()[0],
            datetime.min.time()
        )

        self.assertEqual(
            time_helper.parse_date("yesterday"),
            yesterday
        )
        self.assertIsInstance(
            time_helper.parse_date("yesterday"),
            datetime
        )

    def test_parse_string(self):
        self.assertEqual(
            time_helper.parse_date("20241212"),
            datetime(2024, 12, 12)
        )


if __name__ == '__main__':
    unittest.main()
