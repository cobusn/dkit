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

from ...data import msgpack_utils
from .. import source, sink
from ...data.iteration import chunker


class MsgpackSource(source.AbstractMultiReaderSource):

    def __init__(self, reader_list, field_names=None, encoder=msgpack_utils.MsgpackEncoder):
        super().__init__(reader_list, field_names)
        self.encoder = encoder()

    def iter_some_fields(self, field_names):
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for chunk in self.encoder.unpacker(o_reader):
                    yield from (
                        {k: r[k] for k in field_names}
                        for r in chunk
                    )
                    self.stats.increment(len(chunk))
            else:
                with o_reader.open() as in_file:
                    for chunk in self.encoder.unpacker(in_file):
                        yield from (
                            {k: r[k] for k in field_names}
                            for r in chunk
                        )
                        self.stats.increment(len(chunk))
        self.stats.stop()

    def iter_all_fields(self):
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for chunk in self.encoder.unpacker(o_reader):
                    yield from chunk
                    self.stats.increment(len(chunk))
            else:
                with o_reader.open() as in_file:
                    for chunk in self.encoder.unpacker(in_file):
                        yield from chunk
                        self.stats.increment(len(chunk))
        self.stats.stop()


class LegacyMsgpackSource(source.AbstractMultiReaderSource):

    def __init__(self, reader_list, field_names=None, encoder=msgpack_utils.MsgpackEncoder):
        super().__init__(reader_list, field_names)
        self.encoder = encoder()

    def iter_some_fields(self, field_names):
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                yield from (
                    {k: r[k] for k in field_names}
                    for r in self.encoder.unpacker(o_reader)
                )
                self.stats.increment()
            else:
                with o_reader.open() as in_file:
                    yield from (
                        {k: r[k] for k in field_names}
                        for r in self.encoder.unpacker(in_file)
                    )
                    self.stats.increment()
        self.stats.stop()

    def iter_all_fields(self):
        self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for row in self.encoder.unpacker(o_reader):
                    yield row
                    self.stats.increment()
            else:
                with o_reader.open() as in_file:
                    for row in self.encoder.unpacker(in_file):
                        yield row
                        self.stats.increment()
        self.stats.stop()


class delete_me__MsgpackSink(sink.FileIteratorSink):
    """
    Serialize Dictionary Line to msgpack

    http://msgpack.org/index.html/
    """
    def __init__(self, writer, field_names=None):
        super().__init__(writer)
        self.field_names = field_names
        if field_names is not None:
            self.process_line = self.__process_line_selected_fields
        self.encoder = __import__("msgpack")

    def __process_line_selected_fields(self, entry, out_stream):
        """
        Monkey patching waring...
        """
        out_record = {k: entry[k] for k in self.field_names}
        self.encoder.pack(out_record, out_stream)

    def process_line(self, entry, out_stream):
        self.encoder.pack(entry, out_stream)


class MsgpackSink(sink.AbstractSink):
    """
    Serialize Dictionary Line to msgpack

    http://msgpack.org/index.html/
    """
    def __init__(self, writer, field_names=None, chunk_size=5000, encoder=None,):
        super().__init__()
        self.writer = writer
        self.field_names = field_names
        self.encoder = encoder or msgpack_utils.MsgpackEncoder()
        self.chunk_size = chunk_size

    def get_list(self, chunk):
        if self.field_names:
            field_names = self.field_names
            return [
                {k: row[k] for k in field_names}
                for row in chunk
            ]
        else:
            return list(chunk)

    def process(self, the_iterator):
        self.stats.start()
        if self.writer.is_open:
            for chunk in chunker(the_iterator, size=self.chunk_size):
                c = self.get_list(chunk)
                self.writer.write(self.encoder.pack(c))
                self.stats.increment(len(c))
        else:
            with self.writer.open() as out_stream:
                for chunk in chunker(the_iterator, size=self.chunk_size):
                    c = self.get_list(chunk)
                    out_stream.write(self.encoder.pack(c))
                    self.stats.increment(len(c))
        self.stats.stop()
        return self
