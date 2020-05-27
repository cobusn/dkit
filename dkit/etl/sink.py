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

"""
Sink writers.

Todo
====

* implement chunk_size
"""

import csv
import json
import datetime
import pickle
import decimal
import html
import os

from ..utilities import log_helper
from ..utilities import instrumentation
from ..utilities import iff
from ..data import json_utils as ju
from ..utilities.iter_helper import chunker
from ..utilities.cmd_helper import LazyLoad

# Deferred modules
# cryptography = LazyLoad("cryptography")
import cryptography

# CONSTANTS
DATE_FMT = '%Y-%m-%d'
ISO8601_FMT = '%Y-%m-%dT%H:%M:%SZ'


class Sink(object):
    """
    Abstract base class for sink objects
    """
    def __init__(self, logger=None, log_template=None):
        self.logger = logger if logger else log_helper.null_logger(self.__class__.__name__)
        self.log_template = log_template if log_template else\
            "Wrote ${counter} after ${seconds} seconds."
        self.stats = instrumentation.CounterLogger(
            logger=self.logger,
            log_template=self.log_template
        )

    def process(self, the_iterator):
        pass

    def __call__(self, the_iterator):
        self.process(the_iterator)


class FileIteratorSink(Sink):
    """
    Abstract base class
    """
    def __init__(self, writer, lineterminator="\n", logger=None, log_template=None):
        super(FileIteratorSink, self).__init__(logger=logger, log_template=log_template)
        self.writer = writer
        self.lineterminator = lineterminator

    def process(self, the_iterator):
        """
        controlling proces loop
        """
        stats = self.stats.start()
        if self.writer.is_open:
            out_stream = self.writer.stream
            for entry in the_iterator:
                if stats.value == 0:
                    self.process_first(entry, out_stream)
                self.process_line(entry, out_stream)
                stats.increment()
            self.process_last(out_stream)
        else:
            with self.writer.open() as out_stream:
                for entry in the_iterator:
                    if self.stats.value == 0:
                        self.process_first(entry, out_stream)
                    self.process_line(entry, out_stream)
                    stats.increment()
                self.process_last(out_stream)
        stats.stop()
        return self

    def process_first(self, entry, out_stream):
        """called before first row is processed"""
        pass

    def process_last(self, out_stream):
        """called after last row is processed"""
        pass

    def process_line(self, out_stream):
        raise NotImplementedError


class CsvDictSink(FileIteratorSink):
    """
    Serialize Dictionary Line format to CSV
    """
    def __init__(self, writer, field_names=None, delimiter=",", write_headings=True,
                 lineterminator="\n", logger=None, log_template=None):
        super(CsvDictSink, self).__init__(writer, lineterminator=lineterminator,
                                          logger=logger, log_template=log_template)
        self.delimiter = delimiter
        self.field_names = field_names
        self.write_headings = write_headings

    def process_first(self, entry, out_stream):
        """
        set up writer
        """
        if self.field_names is None:
            self.field_names = entry.keys()
        self.csv_writer = csv.DictWriter(
            out_stream, self.field_names,
            extrasaction='ignore',
            delimiter=self.delimiter,
            lineterminator=self.lineterminator
        )
        if self.write_headings:
            self.csv_writer.writeheader()

    def process_line(self, entry, out_stream):
        self.csv_writer.writerow(entry)


class HtmlTableSink(FileIteratorSink):
    """
    serialize dictionary like records to html tables
    """
    def __init__(self, writer, field_names=None, logger=None, log_template=None,
                 write_headings=True):
        self.write_headings = write_headings
        self.field_names = field_names
        super().__init__(writer, logger=logger, log_template=log_template)

    def __write_headings(self, entry, out_stream):
        """write headings"""
        out_stream.write("<tr>")
        if self.field_names is not None:
            row = "<th>" + "</th><th>".join(self.field_names)
        else:
            row = "<th>" + "</th><th>".join(entry.keys())
        out_stream.write(row)
        out_stream.write("</th></tr>" + self.lineterminator)

    def __write_data(self, entry, out_stream):
        """write data row"""

    def process_first(self, entry, out_stream):
        """
        before first record is processed
        """
        out_stream.write("<table>" + self.lineterminator)
        if self.write_headings:
            self.__write_headings(entry, out_stream)

    def process_line(self, entry, out_stream):
        out_stream.write("<tr>")
        if self.field_names is not None:
            values = [html.escape(entry[k]) for k in self.field_names]
            row = "<td>" + "</td><td>".join(values)
        else:
            row = "<td>" + "</td><td>".join(
                [html.escape(i) for i in entry.values()]
            )
        out_stream.write(row)
        out_stream.write("</td></tr>" + self.lineterminator)

    def process_last(self, out_stream):
        out_stream.write("</table>" + self.lineterminator)


def default_encoder(obj):
    """
    convert python object to reasonable defaults.
    """
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        raise TypeError("Encountered type: {}".format(type(obj)))


class JsonlSink(FileIteratorSink):
    """
    Serialize Dictionary Line to JsonLines

    http://jsonlines.org/
    """
    def __init__(self, writer, logger=None, log_template=None):
        super().__init__(writer, logger=logger, log_template=log_template)
        self.encoder = ju.JsonSerializer(
            ju.DateTimeCodec(),
            ju.DateCodec(),
            ju.Decimal2FloatCodec()
        )

    def process_line(self, entry, stream):
        stream.write(
            self.encoder.dumps(
                entry,
            ) + self.lineterminator
        )


class JsonlSafeSink(FileIteratorSink):
    """
    Serialize Dictionary Line to JsonLines

    convert unknown objects to str and decimal to float
    """
    def __init__(self, writer, logger=None, log_template=None):
        super().__init__(writer, logger=logger, log_template=log_template)
        self.encoder = ju.JsonSerializer(
            ju.DateTimeStrCodec(),
            ju.DateStrCodec(),
            ju.Decimal2FloatCodec()
        )

    def process_line(self, entry, stream):
        stream.write(
            self.encoder.dumps(
                entry,
            ) + self.lineterminator
        )


class JsonSink(Sink):
    """
    Abstract base class
    """
    def __init__(self, writer, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        self.writer = writer

    def process(self, the_iterator):
        """
        controlling proces loop
        """
        stats = self.stats.start()
        if self.writer.is_open:
            lst = list(the_iterator)
            json.dump(lst, self.writer)
            stats.increment(len(lst))
        else:
            with self.writer.open() as out_stream:
                lst = list(the_iterator)
                json.dump(
                    lst,
                    out_stream,
                    default=default_encoder
                  )
                stats.increment(len(lst))
        stats.stop()
        return self


class PickleSink(Sink):
    """
    Serialize to Pickle
    """
    def __init__(self, writer, chunk_size=5000, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        self.writer = writer
        self.chunk_size = chunk_size

    def _write_chunks(self, the_iterator, destination):
        iff_stream = iff.IFFWriter(destination)
        for chunk in chunker(the_iterator, self.chunk_size):
            c = list(chunk)
            iff_stream.write(
                pickle.dumps(c)
            )
            self.stats.increment(len(c))

    def process(self, the_iterator):
        self.stats.start()
        if self.writer.is_open:
            self._write_chunks(the_iterator, self.writer)
        else:
            with self.writer.open() as out_stream:
                self._write_chunks(the_iterator, out_stream)
        self.stats.stop()
        return self


class EncryptSink(Sink):
    """
    Serialize to AES encrypted
    """
    def __init__(self, file_name, key, compress=None, serde=None,
                 chunk_size=5000, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        self.file_name = file_name
        self.chunk_size = chunk_size
        # [ser]ializer default to pickle
        self.serde = serde or pickle
        self.compress = compress
        self.key = key
        self.iff_stream = None

    def process(self, the_iterator):
        from cryptography.fernet import Fernet
        fernet = Fernet(self.key)
        with open(self.file_name, "bw") as destination:
            iff_stream = iff.IFFWriter(destination)
            self.stats.start()
            for chunk in chunker(the_iterator, self.chunk_size):
                c = list(chunk)
                if self.compress:
                    iff_stream.write(
                        fernet.encrypt(
                            self.compress.compress(
                                self.serde.dumps(c)
                            )
                        )
                    )
                else:
                    iff_stream.write(
                        fernet.encrypt(
                            self.serde.dumps(c)
                        )
                    )
                self.stats.increment(len(c))
            self.stats.stop()


class NullSink(Sink):
    """
    write to /dev/null
    """
    def __init__(self, logger=None, log_template=None):
        super().__init__(logger=logger, log_template=log_template)
        self.writer = os.devnull

    def process(self, the_iterator):
        self.stats.start()
        self._write_chunks(the_iterator, self.writer)
        self.stats.stop()
        return self


def load(uri: str, logger=None) -> Sink:
    """
    parse uri string and open + return sink
    """
    from . import utilities as etlu
    return etlu.open_sink(uri, logger)
