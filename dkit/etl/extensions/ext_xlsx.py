# Copyright (c) 2022 Cobus Nel
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

from .. import source, sink, DEFAULT_LOG_TRIGGER
from ...utilities.cmd_helper import LazyLoad
from datetime import datetime, date
from decimal import Decimal
import logging
from typing import List, Callable
pyxl = LazyLoad("openpyxl")

logger = logging.getLogger(__name__)


class XlsxSink(sink.AbstractSink):
    """
    Serialize Dictionary Line to Excel using openpyxl.

    [openpyxl](https://openpyxl.readthedocs.io/en/default/optimized.html)
    """
    def __init__(self, file_name: str, field_names: List[str] = None):
        super().__init__()
        self.file_name = file_name
        self.field_names = field_names

    def __convert(self, value):
        if isinstance(value, (str, int, float, datetime, date, Decimal)):
            return value
        else:
            return str(value)

    def process_dict(self, the_dict):
        """
        Each entry in the dictionary is written to a separate
        worksheet.
        """
        stats = self.stats.start()
        wb = pyxl.Workbook(write_only=True)

        for name, the_iterable in the_dict.items():
            logger.info(f"writing sheet '{name}'")
            ws = wb.create_sheet(name)
            for i, row in enumerate(the_iterable):
                if i == 0:
                    if self.field_names is not None:
                        field_names = self.field_names
                    else:
                        field_names = list(row.keys())
                    ws.append(field_names)
                ws.append([self.__convert(row[k]) for k in field_names])
                stats.increment()

        wb.save(self.file_name)
        stats.stop()
        return self

    def process(self, the_iterable):
        stats = self.stats.start()
        wb = pyxl.Workbook(write_only=True)
        ws = wb.create_sheet()

        for i, row in enumerate(the_iterable):
            if i == 0:
                if self.field_names is not None:
                    field_names = self.field_names
                else:
                    field_names = list(row.keys())
                ws.append(field_names)
            ws.append([self.__convert(row[k]) for k in field_names])
            stats.increment()
        wb.save(self.file_name)
        stats.stop()
        return self

    def close(self):
        pass


class XLSXSource(source.AbstractSource):
    """
    Read data from XLSX file.

    This class assumes that the column headings is in the first row.

    skip_lines is ignored if field_names is None

    Arguments:
        - file_name_list: provide headings
        - work_sheet: name of worksheet
        - field_names: headings
        - skip_lines: skip n lines before headings
        - log_trigger: number of lines on which to log progress
        - stop_fn: stop when this function return True

    """

    def __init__(
        self,
        file_name_list: List[str],
        work_sheet: str = None,
        field_names: List[str] = None,
        skip_lines: int = 0,
        log_trigger: int = DEFAULT_LOG_TRIGGER,
        stop_fn: Callable = None
    ):
        super().__init__(log_trigger=log_trigger)
        self.file_names = file_name_list
        self.headings = field_names
        self.skip_lines = skip_lines
        self.work_sheet = work_sheet
        self.stop_fn = stop_fn

    def __get_headings(self, rows):
        if self.headings:
            return self.headings
        else:
            heading_row = next(rows)
            return [str(i.value) for i in heading_row]

    def __iter__(self):
        stats = self.stats.start()
        for file_name in self.file_names:
            logger.info(f"reading: {file_name}")
            wb = pyxl.load_workbook(file_name, read_only=True)
            if self.work_sheet is None:
                ws_name = wb.sheetnames[0]
            else:
                ws_name = self.work_sheet

            ws = wb[ws_name]
            rows = ws.rows

            # skip lines specified
            for i in range(self.skip_lines):
                next(rows)

            # get headings
            headings = self.__get_headings(rows)
            row = next(rows)
            while row:
                try:
                    stats.increment()
                    candidate = dict(zip(headings, [i.value for i in row]))
                    for key, value in candidate.items():
                        if isinstance(value, str):
                            # Fix occasional strange unicode issues..
                            candidate[key] = value.encode('ascii', 'ignore')\
                                .decode('utf-8', errors="ignore")
                    if self.stop_fn:
                        if self.stop_fn(candidate):
                            # stop if stop_fn return true.
                            break
                    yield candidate
                    row = next(rows)
                except StopIteration:
                    row = None
        stats.stop()

    def reset(self):
        """not applicable"""
        pass

    def close(self):
        """not applicable"""
        pass


def read_xlsx(file_name, work_sheet=None, skip_lines=0, stop_fn=None):
    """
    Convenience function to read tables from a worksheet
    """
    yield from XLSXSource([file_name], work_sheet, skip_lines=skip_lines,
                          stop_fn=stop_fn)
