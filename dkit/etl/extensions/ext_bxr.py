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

from .. import DEFAULT_LOG_TRIGGER
from ...data import bxr
from .. import source, sink


class BXRSink(sink.FileIteratorSink):
    """
    Serialize Dictionary Line to BXR
    """
    def __init__(self, writer, field_names=None, logger=None, log_template=None):
        super().__init__(writer, logger=logger, log_template=log_template)
        self.field_names = field_names
        if field_names is not None:
            self.process_line = self.__process_line_selected_fields

    def __process_line_selected_fields(self, entry, out_stream):
        """
        Monkey patching waring...
        """
        bxr.dump(
            {k: entry[k] for k in self.field_names},
            out_stream
        )

    def process_line(self, entry, stream):
        bxr.dump(entry, stream)


class BXRSource(source.AbstractMultiReaderSource):
    """
    read records from a JSONL source

    :reader_list: list of reader objects
    :field_names: (optional) list of fields to extract
    :logger: (optional) logger instance
    :log_template: (optional) python log template
    :skip_lines: (optional) number of lines to skip at start of file
    """

    def __init__(self, reader_list, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0):
        super().__init__(reader_list, field_names, logger=logger, log_template=log_template,
                         log_trigger=log_trigger)

    def iter_field_names(self, field_names):
        """
        called when specific fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for line in bxr.load_iter(o_reader):
                    yield({k: v for k, v in line.items() if k in field_names})
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for line in bxr.load_iter(in_file):
                        yield({k: v for k, v in line.items() if k in field_names})
                        stats.increment()
        stats.stop()

    def iter_all_fields(self):
        """
        called when all fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for line in bxr.load_iter(o_reader):
                    yield line
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for line in bxr.load_iter(in_file):
                        yield line
                        stats.increment()
        stats.stop()
