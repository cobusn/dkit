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
import sys
import datetime
sys.path.insert(0, "..") # noqa

from dkit.etl.extensions import ext_xlsx
from test_config import DATA_TYPES


class TestXLSXSource(unittest.TestCase):

    def setUp(self):
        self.excel1 = os.path.join("input_files", "excel_1.xlsx")
        self.excel_break = os.path.join("input_files", "excel_break.xlsx")

    def _test_data_types(self, as_dict):
        self.assertEqual(as_dict[0]["DATE"], datetime.datetime(2010, 1, 2, 0, 0))
        self.assertEqual(as_dict[1]["FLOAT"], 2.2)
        self.assertEqual(as_dict[1]["INT"], 1)

    def test_provided_headings(self):
        """test with provided headings"""
        headings = ["INT", "FLOAT", "DATE", "STR"]
        t = ext_xlsx.XLSXSource([self.excel1], field_names=headings, skip_lines=1)
        self._test_data_types(list(t))

    def test_multiple_files(self):
        t = ext_xlsx.XLSXSource([self.excel1, self.excel1])
        self._test_data_types(list(t))
        self.assertEqual(list(t)[2]["DATE"], datetime.datetime(2010, 1, 4, 0, 0))

    def test_exit_fn(self):
        def stop_fn(row):
            if row["INT"] is None:
                return True
            return False

        t = list(ext_xlsx.XLSXSource([self.excel_break], stop_fn=stop_fn))
        self.assertEqual(len(t), 3)
        self.assertEqual(
            t[-1],
            {'INT': 3, 'FLOAT': 3.1, 'DATE': datetime.datetime(2010, 1, 4, 0, 0), 'STR': 'C'}
        )

    def test_worksheet_name(self):
        t = ext_xlsx.XLSXSource([self.excel1], work_sheet="Sheet2")
        self._test_data_types(list(t))
        self.assertEqual(list(t)[2]["DATE"], datetime.datetime(2010, 1, 10, 0, 0))

    def test_reset(self):
        """
        Confirm that the iterater can be restarted after a reset
        """
        t = ext_xlsx.XLSXSource([self.excel1])
        ll = list(t)
        t.reset()
        l2 = list(t)
        self.assertEqual(len(ll), len(l2))


class TestXlsxSink(unittest.TestCase):

    def test_data_types(self):
        """
        test writing various data types to excel.
        """
        g_data = (DATA_TYPES for i in range(10))
        snk_xlsx = ext_xlsx.XlsxSink(os.path.join("output", "xlsx_data_types.xlsx"))
        snk_xlsx.process(g_data)

    def test_process_dict(self):
        """
        test process_dict method
        """
        g_data = list((DATA_TYPES for i in range(10)))
        snk_xlsx = ext_xlsx.XlsxSink(os.path.join("output", "xlsx_multi_sheet.xlsx"))
        sheets = {"page 1": g_data, "page 2": g_data}
        snk_xlsx.process_dict(sheets)

    def test_10000_rows(self):
        """
        Test writing 10 000 rows to xlsx file
        """
        g_data = (DATA_TYPES for i in range(10000))
        snk_xlsx = ext_xlsx.XlsxSink(os.path.join("output", "xlsx_10000_rows.xlsx"))
        snk_xlsx.process(g_data)

    def test_sorted_fields(self):
        """
        Test writing xlsx file with specified field list
        """
        g_data = (DATA_TYPES for i in range(10))
        field_list = list(reversed(sorted(DATA_TYPES.keys())[:3]))
        snk_xlsx = ext_xlsx.XlsxSink(os.path.join("output", "xlsx_field_list.xlsx"),
                                     field_names=field_list)
        snk_xlsx.process(g_data)


if __name__ == '__main__':
    unittest.main()
