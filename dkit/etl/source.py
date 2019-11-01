# Copyright (c) 2017 Cobus Nel
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

"""
Sources are ETL artifacts that implement interaction with
encodings independent of the storage mechanism
"""

import csv
import glob
import json
import _pickle
import string
from contextlib import contextmanager

from . import MESSAGES
from . import DEFAULT_LOG_TRIGGER
from ..utilities import (instrumentation, log_helper, iff)
from ..parsers import uri_parser
from ..data import json_utils as ju


class AbstractSource(object):
    """
    abstract source class

    :param logger: (optional) logger instance
    :param log_template: (optional) python logging template
    """

    def __init__(self, logger=None, log_template: str = None,
                 log_trigger: int = DEFAULT_LOG_TRIGGER, **kwargs):
        self.logger = logger if logger else log_helper.null_logger()
        if log_template is None:
            log_template = "Read ${counter} after ${seconds} seconds."

        # stats
        self.stats = instrumentation.CounterLogger(
            self.logger,
            log_template=log_template,
            trigger=log_trigger
        )

    def as_data_frame(self):
        """all data as Pandas data frame"""
        raise NotImplementedError

    def reset(self):
        pass

    @property
    def nrows(self):
        """number of rows"""
        raise NotImplementedError

    def __iter__(self):
        raise NotImplementedError


class FileListingSource(AbstractSource):
    """
    file-name generator

    :param glob_list: list of globs
    :logger: logger instance
    :log_template: log template
    """
    def __init__(self, glob_list, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER):
        if not isinstance(glob_list, (list, set)):
            message = string.Template(MESSAGES.LIST_REQUIRED).substitute({"parameter": "glob_list"})
            raise ValueError(message)
        self.glob_list = glob_list
        super().__init__(logger=logger, log_template=log_template, log_trigger=log_trigger)

    def __iter__(self):
        self.stats.start()
        for glob_item in self.glob_list:
            for file_name in glob.glob(glob_item):
                self.stats.increment()
                yield file_name
        self.stats.stop()


class AbstractRowSource(AbstractSource):

    def __init__(self, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0, **kwargs):
        super().__init__(logger=logger, log_template=log_template, log_trigger=log_trigger)
        self.field_names = field_names
        self.skip_lines = skip_lines

    def iter_all_fields(self):
        """iterate with all fields"""
        raise NotImplementedError

    def iter_some_fields(self, field_names):
        """iterater of dicts that contain specified fields"""
        raise NotImplementedError

    def iter_one_field(self, field_name):
        """iterater of dicts that contain specified fields"""
        yield from self.iter_some_fields([field_name])

    def __iter__(self):
        """
        yield rows
        """
        if self.field_names is None:
            yield from self.iter_all_fields()
        elif len(self.field_names) == 1:
            yield from self.iter_one_field(self.field_names[0])
        else:
            yield from self.iter_some_fields(self.field_names)


class AbstractMultiReaderSource(AbstractRowSource):
    """
    base class for sources that accept a list of readers.
    """
    def __init__(self, reader_list, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0, **kwargs):
        super().__init__(logger=logger, log_template=log_template, log_trigger=log_trigger,
                         field_names=field_names, skip_lines=skip_lines)
        self.reader_list = reader_list

    def reset(self):
        """
        Reset all files
        """
        for o_reader in self.reader_list:
            if o_reader.is_open:
                o_reader.seek(0)

    def close(self):
        for reader in self.reader_list:
            if hasattr(reader, "close"):
                reader.close()
                print("closing")


class PickleSource(AbstractMultiReaderSource):
    """
    read records from a pickled source

    :reader_list: list of reader objects
    :field_names: (optional) list of fields to extract
    :logger: (optional) logger instance
    :log_template: (optional) python log template
    """

    def __init__(self, reader_list, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, **kwargs):
        super().__init__(reader_list, field_names, logger=logger, log_template=log_template,
                         log_trigger=log_trigger, **kwargs)

    def iter_chunk(self, open_reader):
        iff_stream = iff.IFFReader(open_reader)
        for chunk in iff_stream:
            retval = _pickle.loads(chunk)
            yield from retval
            self.stats.increment(len(retval))

    def iter_some_fields(self, field_names):
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                yield from (
                    {k: r[k] for k in self.field_names}
                    for r in self.iter_chunk(o_reader)
                )
            else:
                with o_reader.open() as in_file:
                    yield from (
                        {k: r[k] for k in self.field_names}
                        for r in self.iter_chunk(in_file)
                    )
        self.stats.stop()

    def iter_all_fields(self):
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                yield from self.iter_chunk(o_reader)
            else:
                with o_reader.open() as in_file:
                    yield from self.iter_chunk(in_file)
        stats.stop()


class JsonSource(AbstractMultiReaderSource):
    """
    read records from a JSON source

    Args:
        * reader_list: list of reader objects
        * field_names: (optional) list of fields to extract
        * logger: (optional) logger instance
        * log_template: (optional) python log template
        * skip_lines: (optional) number of lines to skip at start of file
    """

    def __init__(self, reader_list, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0, **kwargs):
        super().__init__(reader_list, field_names, logger=logger, log_template=log_template,
                         log_trigger=log_trigger, **kwargs)
        self.json = ju.JsonSerializer(
            ju.DateCodec(),
            ju.DateTimeCodec(),
            encoder=json
        )

    def iter_some_fields(self, field_names):
        """
        called when specific fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for row in self.json.load(o_reader):
                    yield({k: row[k] for k in field_names})
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for row in self.json.load(in_file):
                        yield({k: row[k] for k in field_names})
                        stats.increment()
        stats.stop()

    def iter_all_fields(self):
        """
        called when all fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for row in self.json.load(o_reader):
                    yield(row)
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for row in self.json.load(in_file):
                        yield(row)
                        stats.increment()
        stats.stop()


class JsonlSource(AbstractMultiReaderSource):
    """
    read records from a JSONL source

    Args:
        * reader_list: list of reader objects
        * chunk_size: bytes to read (hint for file.readlines)
        * field_names: (optional) list of fields to extract
        * logger: (optional) logger instance
        * log_template: (optional) python log template
        * skip_lines: (optional) number of lines to skip at start of file
    """

    def __init__(self, reader_list, chunk_size=1024*1024*5, field_names=None, logger=None,
                 log_template=None, log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0, **kwargs):
        super().__init__(reader_list, field_names, logger=logger, log_template=log_template,
                         log_trigger=log_trigger, **kwargs)
        self.json = ju.JsonSerializer(
            ju.DateCodec(),
            ju.DateTimeCodec(),
            encoder=json
        )
        self.chunk_size = chunk_size

    def parse_chunk(self, in_file):
        lines = in_file.readlines(self.chunk_size)
        while lines:
            yield from self.json.loads(f"[{','.join(lines)}]")
            self.stats.increment(len(lines))
            lines = in_file.readlines(self.chunk_size)

    def iter_some_fields(self, field_names):
        """
        called when specific fields specified
        """
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                yield from (
                    {
                        k: the_dict[k]
                        for k in field_names
                    }
                    for the_dict in self.parse_chunk(o_reader)
                )
            else:
                with o_reader.open() as in_file:
                    yield from (
                        {
                            k: the_dict[k]
                            for k in field_names
                        }
                        for the_dict in self.parse_chunk(in_file)
                    )
        self.stats.stop()

    def iter_all_fields(self):
        """
        called when all fields specified
        """
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                yield from self.parse_chunk(o_reader)
            else:
                with o_reader.open() as in_file:
                    yield from self.parse_chunk(in_file)
        self.stats.stop()


class deprecated_JsonlSource(AbstractMultiReaderSource):
    """
    read records from a JSONL source

    Args:
        * reader_list: list of reader objects
        * field_names: (optional) list of fields to extract
        * logger: (optional) logger instance
        * log_template: (optional) python log template
        * skip_lines: (optional) number of lines to skip at start of file
    """

    def __init__(self, reader_list, field_names=None, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, skip_lines=0):
        super().__init__(reader_list, field_names, logger=logger, log_template=log_template,
                         log_trigger=log_trigger)
        self.json = json

    def iter_some_fields(self, field_names):
        """
        called when specific fields specified
        """
        loader = self.json
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for line in o_reader:
                    the_dict = loader.loads(line.rstrip("\n|\r"))
                    yield({k: the_dict[k] for k in field_names})
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for line in in_file:
                        the_dict = loader.loads(line.rstrip("\n|\r"))
                        yield({k: the_dict[k] for k in field_names})
                        stats.increment()
        stats.stop()

    def iter_all_fields(self):
        """
        called when all fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for line in o_reader:
                    stats.increment()
                    yield(json.loads(line.rstrip("\n|\r")))
            else:
                with o_reader.open() as in_file:
                    for line in in_file:
                        stats.increment()
                        yield(json.loads(line.rstrip("\n|\r")))
        stats.stop()


class CsvDictSource(AbstractMultiReaderSource):
    """
    read records from CSV sources

    :reader_list: list of reader objects
    :field_names: (optional) list of fields to extract
    :delimiter: (optional) defaults to ","
    :logger: (optional) logger instance
    :log_template: (optional) python log template
    :skip_lines: (optional) number of lines to skip at start of file
    """
    def __init__(self, reader_list, field_names=None, delimiter=",",
                 logger=None, log_template=None, log_trigger=DEFAULT_LOG_TRIGGER,
                 skip_lines=0, **kwargs):
        self.delimiter = delimiter
        super().__init__(
            reader_list, field_names, logger=logger, log_template=log_template,
            log_trigger=log_trigger, **kwargs
        )

    def iter_all_fields(self):
        """
        Iterate through files
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for _ in range(self.skip_lines):
                    # skip specified blank lines.
                    next(o_reader)
                csv_in = csv.DictReader(
                    o_reader,
                    self.field_names,
                    delimiter=self.delimiter,
                    skipinitialspace=True
                )
                for row in csv_in:
                    yield(row)
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for _ in range(self.skip_lines):
                        next(in_file)
                    csv_in = csv.DictReader(in_file, delimiter=self.delimiter)
                    for row in csv_in:
                        yield(row)
                        stats.increment()
        stats.stop()

    def iter_some_fields(self, field_names):
        """
        Iterate through files
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for _ in range(self.skip_lines):
                    # skip specified blank lines.
                    next(o_reader)
                csv_in = csv.reader(o_reader, delimiter=self.delimiter, skipinitialspace=True)
                for row in csv_in:
                    yield {k: row[i] for i, k in enumerate(field_names)}
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for _ in range(self.skip_lines):
                        next(in_file)
                    csv_in = csv.reader(in_file, self.field_names, delimiter=self.delimiter)
                    for row in csv_in:
                        yield {k: row[i] for i, k in enumerate(field_names)}
                        stats.increment()
        stats.stop()


class XmlRpcSource(AbstractRowSource):

    def __init__(self, server, method, params, logger=None, log_template=None,
                 log_trigger=DEFAULT_LOG_TRIGGER, **kwargs):
        from xmlrpc import client
        super().__init__(logger=logger, log_template=log_template, log_trigger=log_trigger,
                         **kwargs)
        self.server = server
        self.method = method
        self.params = params
        self.proxy = client.ServerProxy(self.server)

    def _call(self, method, l_params):
        """
        Call method with list parameters
        """
        return getattr(self.proxy, method)(*l_params)

    def iter_all_fields(self):
        stats = self.stats.start()
        for row in self._call(self.method, self.params):
            stats.increment()
            yield row
        stats.stop()


@contextmanager
def load(uri: str, skip_lines=0, field_names=None, logger=None, delimiter=","):
    """
    helper function to open a source
    """
    from . import utilities
    try:
        factory = utilities._SourceIterFactory(
            uri_parser.parse(uri), skip_lines, field_names, logger, delimiter
        )
        yield factory
    finally:
        factory.close()
