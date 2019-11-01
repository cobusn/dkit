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
from .. import source, sink, DEFAULT_LOG_TRIGGER
from datetime import datetime, date
from decimal import Decimal


class XlsxSink(sink.Sink):
    """
    Serialize Dictionary Line to Excel using openpyxl.

    [openpyxl](https://openpyxl.readthedocs.io/en/default/optimized.html)
    """
    def __init__(self, file_name, field_names=None, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        self.openpyxl = __import__('openpyxl')
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
        wb = self.openpyxl.Workbook(write_only=True)

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
        wb = self.openpyxl.Workbook(write_only=True)
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
    """

    def __init__(self, file_name_list, work_sheet=None,
                 field_names=None, skip_lines=0, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER):
        super(XLSXSource, self).__init__(logger, log_template=log_template, log_trigger=log_trigger)
        self.openpyxl = __import__('openpyxl')
        self.file_names = file_name_list
        self.__field_names = field_names
        self.skip_lines = skip_lines
        self.work_sheet = work_sheet

    def __get_headings(self, rows):
        if self.__field_names:
            return self.__field_names
        else:
            heading_row = next(rows)
            return [i.value for i in heading_row]

    def __it(self):
        stats = self.stats.start()
        for file_name in self.file_names:
            self.logger.info(file_name)
            wb = self.openpyxl.load_workbook(file_name, read_only=True)
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
            headings = [str(i) for i in self.__get_headings(rows)]
            row = next(rows)
            while row:
                try:
                    stats.increment()
                    # yield {k: v for k,v in zip(headings, [i.value for i in row])}
                    candidate = {k: v for k, v in zip(headings, [i.value for i in row])}
                    for key, value in candidate.items():
                        if isinstance(value, str):
                            # Fix strange unicode issues..
                            candidate[key] = value.encode('ascii', 'ignore')\
                                .decode('utf-8', errors="ignore")
                    yield candidate
                    row = next(rows)
                except StopIteration:
                    row = None
        stats.stop()

    def __iter__(self):
        yield from self.__it()

    def reset(self):
        """
        Does nothing..
        """
        pass

    def close(self):
        pass
