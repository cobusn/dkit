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

from .. import source, sink, DEFAULT_LOG_TRIGGER
from datetime import datetime, date
from decimal import Decimal
import xlrd


class XLSSink(sink.Sink):
    """
    Serialize Dictionary Line to Excel using openpyxl.

    [openpyxl](https://openpyxl.readthedocs.io/en/default/optimized.html)
    """
    def __init__(self, file_name, field_names=None, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        # self.xlrd = __import__('xlrd')
        self.xlrd = xlrd
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
        wb = self.xlrd.open_workbook(self.file_name)

        for name, the_iterable in the_dict.items():
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
        wb = self.xlrd.Workbook(write_only=True)
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


class XLSSource(source.AbstractSource):
    """
    Read data from XLSX file.

    This class assumes that the column headings is in the first row.

    skip_lines is ignored if field_names is None
    """

    def __init__(self, file_name_list, work_sheet=None,
                 field_names=None, skip_lines=0, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER):
        super().__init__(logger, log_template=log_template, log_trigger=log_trigger)
        self.xlrd = xlrd
        # self.xlrd = __import__('xlrd')
        self.file_names = file_name_list
        self.__field_names = field_names
        self.skip_lines = skip_lines
        self.work_sheet = work_sheet

    def __get_headings(self, row):
        if self.__field_names:
            return self.__field_names
        else:
            heading_row = row
            return [i.value for i in heading_row]

    def __it(self):
        stats = self.stats.start()
        for file_name in self.file_names:
            self.logger.info(file_name)
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
                    candidate = {k: v for k, v in zip(headings, [i.value for i in row])}
                    for key, value in candidate.items():
                        if isinstance(value, str):
                            # Fix strange unicode issues..
                            candidate[key] = value.encode('ascii', 'ignore')\
                                .decode('utf-8', errors="ignore")
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
