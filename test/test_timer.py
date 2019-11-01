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

import time
import sys
import unittest
sys.path.insert(0, "..")

from dkit.utilities import instrumentation
import common


class TestTimer(common.TestBase):
    """Test the Timer class"""

    @classmethod
    def setUpClass(cls):
        super(TestTimer, cls).setUpClass()
        cls.duration = 1
        cls.t_obj = instrumentation.Timer()
        cls.t_obj.start()
        cls.started = cls.t_obj.time_started
        time.sleep(1)
        cls.stopped = cls.t_obj.stop().time_stopped

    def test_hours_elapsed(self):
        """test hours elapsed"""
        self.assertAlmostEqual(self.t_obj.hours_elapsed, self.duration/60/60, 2)

    def test_minutes_elapsed(self):
        """test minutes elapsed"""
        self.assertAlmostEqual(self.t_obj.minutes_elapsed, self.duration/60.0, 2)

    def test_seconds_elapsed(self):
        """test seconds elapsed"""
        self.assertAlmostEqual(self.t_obj.seconds_elapsed, self.duration, 2)

    def test_str_elapsed(self):
        """test str elapsed"""
        _ = self.t_obj.str_elapsed

    def test__str__(self):
        self.assertEqual(self.t_obj.str_elapsed, str(self.t_obj))

    def test_seconds_elapsed_running(self):
        """test seconds elapsed while still running"""
        t = instrumentation.Timer()
        t.start()
        time.sleep(self.duration)
        e = t.seconds_elapsed
        self.assertAlmostEqual(e, self.duration, 1)

    def test_hms(self):
        """test `hms_elapsed` property"""
        h, m, s, ms = self.t_obj.hms_elapsed
        d = h*60*60 + m*60 + s + ms/1000.0
        self.assertAlmostEqual(d, self.duration, 1)

    def test_raises_stop(self):
        """Test that errors are raised correctly."""
        t = instrumentation.Timer()
        with self.assertRaises(instrumentation.TimerException) as _:
            t.stop()

    def test_raises_elapsed(self):
        """raise exception if timer not started"""
        t = instrumentation.Timer()
        with self.assertRaises(instrumentation.TimerException) as _:
            _ = t.seconds_elapsed

    def test_raises_started(self):
        """raise exception if timer not started"""
        t = instrumentation.Timer()
        with self.assertRaises(instrumentation.TimerException) as _:
            _ = t.time_started

    def test_raises_stop_method(self):
        """raise exception if timer not stopped when calling time_stopped"""
        t = instrumentation.Timer()
        t.start()
        with self.assertRaises(instrumentation.TimerException) as _:
            t.time_stopped

    def test_timer(self):
        """test duration"""
        self.assertTrue(self.t_obj.seconds_elapsed > 0.299)

    def test_start(self):
        """test result of time_started property"""
        self.assertEqual(self.started, self.t_obj.time_started)

    def test_stop(self):
        """test result of time_stopped property"""
        self.assertEqual(self.stopped, self.t_obj.time_stopped)


if __name__ == '__main__':
    unittest.main()
