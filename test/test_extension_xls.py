# Copyright (c) 2019 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os
import unittest
import sys
sys.path.insert(0, "..") # noqa
import datetime
from dkit.etl.extensions import ext_xls


class TestXLSSource(unittest.TestCase):

    def setUp(self):
        self.excel1 = os.path.join("input_files", "excel_1.xls")
        self.excel_break = os.path.join("input_files", "excel_break.xls")

    def _test_data_types(self, as_dict):
        # self.assertEqual(as_dict[0]["DATE"], datetime.datetime(2010, 1, 2, 0, 0))
        self.assertEqual(as_dict[1]["FLOAT"], 2.2)
        self.assertEqual(as_dict[1]["INT"], 1)

    def test_provided_headings(self):
        """test with provided headings"""
        headings = ["INT", "FLOAT", "DATE", "STR"]
        t = ext_xls.XLSSource([self.excel1], field_names=headings, skip_lines=0)
        self._test_data_types(list(t))

    def test_multiple_files(self):
        t = ext_xls.XLSSource([self.excel1, self.excel1])
        self._test_data_types(list(t))
        # self.assertEqual(list(t)[2]["DATE"], datetime.datetime(2010, 1, 4, 0, 0))

    def test_worksheet_name(self):
        t = ext_xls.XLSSource([self.excel1], work_sheet="Sheet2")
        self._test_data_types(list(t))
        # self.assertEqual(list(t)[2]["DATE"], datetime.datetime(2010, 1, 10, 0, 0))

    def test_reset(self):
        """
        Confirm that the iterater can be restarted after a reset
        """
        t = ext_xls.XLSSource([self.excel1])
        ll = list(t)
        t.reset()
        l2 = list(t)
        self.assertEqual(len(ll), len(l2))

    def test_exit_fn(self):

        def stop_fn(row):
            if row["INT"] is None:
                return True
            return False

        t = list(ext_xls.XLSSource([self.excel_break], stop_fn=stop_fn))
        print(t)
        self.assertEqual(len(t), 3)


if __name__ == '__main__':
    unittest.main()
