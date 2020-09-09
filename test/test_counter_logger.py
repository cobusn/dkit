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
'''
Created on 17 May 2011
'''
import unittest
import common
import sys; sys.path.insert(0, "..")  # noqa

from dkit.utilities.instrumentation import CounterLogger
from dkit.utilities.log_helper import init_stderr_logger
import time


init_stderr_logger()


class TestCounterLogger(common.TestBase):
    """Test the MSDateProcessor class"""

    def setUp(self):
        super(TestCounterLogger, self).setUp()
        self.t_obj = CounterLogger(self.__class__.__name__, trigger=10)

    def test_loop(self):
        """Test CouterLogger() operation."""
        self.t_obj.start()
        x = 0
        while x < 100:
            self.t_obj.increment()
            time.sleep(0.04)
            x += 1
        self.assertEqual(self.t_obj.value, 100)
        self.t_obj.stop()

    def test_template_values(self):
        """Test CounterLogger() template values"""
        self.t_obj.start()
        self.t_obj.trigger = 1
        self.t_obj.log_template = "Log Template hours: ${hours} at ${counter} counted."
        self.t_obj.increment(1)
        self.t_obj.log_template = "Log Template minutes: ${minutes} at ${counter} counted."
        self.t_obj.increment(1)
        self.t_obj.log_template = "Log Template seconds: ${seconds} at ${counter} counted."
        self.t_obj.increment(1)
        self.t_obj.log_template = "Log Template time: ${strtime} at ${counter} counted."
        self.t_obj.increment(1)
        self.t_obj.start()

    def test_log_actions(self):
        """Test CounterLogger log actions"""
        self.t_obj.start()
        self.t_obj.increment()
        self.t_obj.debug()
        self.t_obj.info()
        self.t_obj.warning()
        self.t_obj.error()
        self.t_obj.stop()

    def test_log_actions_with_param(self):
        """Test CounterLogger() log with parameters"""
        s_test = "Logged ${counter} times at ${seconds}"
        self.t_obj.start()
        self.t_obj.increment()
        self.t_obj.debug(s_test)
        self.t_obj.info(s_test)
        self.t_obj.warning(s_test)
        self.t_obj.error(s_test)
        self.t_obj.stop()

    def tearDown(self):
        super(TestCounterLogger, self).tearDown()
        self.t_obj = None


if __name__ == '__main__':
    unittest.main()
