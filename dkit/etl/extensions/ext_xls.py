# Copyright (c) 2020 Cobus Nel
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

import logging
from typing import List, Callable

from .. import source, DEFAULT_LOG_TRIGGER
from ...utilities.cmd_helper import LazyLoad


xlrd = LazyLoad("xlrd")


logger = logging.getLogger(__name__)


class XLSSource(source.AbstractSource):
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
        self.xlrd = xlrd
        self.file_names = file_name_list
        self.__field_names = field_names
        self.skip_lines = skip_lines
        self.work_sheet = work_sheet
        self.stop_fn = stop_fn if stop_fn else lambda x: False

    def __get_headings(self, row):
        if self.__field_names:
            return self.__field_names
        else:
            heading_row = row
            return [i.value for i in heading_row]

    def __it(self):
        stats = self.stats.start()
        empty = [xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK]

        for file_name in self.file_names:
            logger.info(file_name)
            wb = self.xlrd.open_workbook(file_name)
            if self.work_sheet is None:
                ws = wb.sheet_by_index(0)
            else:
                ws = wb.sheet_by_name(self.work_sheet)

            nrows = ws.nrows
            self.reset()

            # get headings
            headings = [str(i) for i in self.__get_headings(ws.row(self.idx_row))]
            self.idx_row += 1

            while self.idx_row < nrows:
                try:
                    row = ws.row(self.idx_row)
                    stats.increment()
                    row = [None if i.ctype in empty else i.value for i in row]
                    candidate = {k: v for k, v in zip(headings, row)}
                    for key, value in candidate.items():
                        if isinstance(value, str):
                            # Fix strange unicode issues..
                            candidate[key] = value.encode('ascii', 'ignore')\
                                .decode('utf-8', errors="ignore")

                    if self.stop_fn(candidate):
                        # stop if stop_fn return true.
                        break

                    yield candidate
                    self.idx_row += 1
                except StopIteration:
                    row = None
        stats.stop()
        self.idx_row = 0

    def __iter__(self):
        yield from self.__it()

    def reset(self):
        self.idx_row = self.skip_lines

    def close(self):
        pass
