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

"""
Routines and classes for handling Apache Avro data

References:

* https://avro.apache.org/
* https://fastavro.readthedocs.io/en/latest/
"""
from itertools import islice, chain
from typing import Iterable

from fastavro import (
    writer as avro_writer,
    reader as avro_reader,
    parse_schema
)

from .. import sink, writer, source
from ..model import Entity

"""
Avro core types:

string          str
int,long        int
boolean         bool
float,double    float
null            None
bytes           bytes
int             types.Int32
float           types.Float32
"""

# convert cannonical to avro
AVRO_TYPEMAP = {
    "float":   "double",
    "double":  "double",
    "integer": "long",
    "int8":    "int",
    "int16":   "int",
    "int32":   "int",
    "int64":   "long",
    "string":  "string",
    "boolean": "boolean",
    "binary":  "bytes",
    "datetime": {
        "type": "long",
        "logicalType": "timestamp-millis"
    },
    #   "date":                   fixme
    #   "datetime"                fixme:
    #   "Decimal":                fixme
}


def make_avro_schema(cannonical_schema: Entity, name="auto.generated"):
    """create an Avro schema from cannonical Entity"""
    fields = []
    validator = cannonical_schema.as_entity_validator()
    for name, definition in validator.schema.items():
        fields.append(
            {
                "name": name,
                "type": AVRO_TYPEMAP[definition["type"]]
            }
        )
    return {
        "type": "record",
        "name": name,
        "fields": fields,
    }


def infer_avro_schema(iterable, n=100):
    """
    infer schema from iterable

    returns:
        * a reconstructed iterable
        * entity object
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    schema = make_avro_schema(
        Entity.from_iterable(buffer, p=1.0, k=n)
    )
    return schema, chain(buffer, i)


class AvroSink(sink.FileIteratorSink):
    """
    Write data in Apache Avro format.

    The class can automatically generate a schema,
    or use a schema provided in cannonical format.
    """
    def __init__(self, writer, schema: Entity = None, schema_name="generated",
                 codec="snappy"):
        super().__init__(writer)
        self.schema: Entity = schema
        self.schema_name = schema_name
        self.codec = codec

    def __write(self, stream: writer.Writer, the_iterable: Iterable):
        """internal write function"""
        schema, iterable = self._get_schema(the_iterable)
        parsed_schema = parse_schema(schema)
        avro_writer(
            stream,
            parsed_schema,
            the_iterable,
            codec=self.codec
        )

    def _get_schema(self, the_iterable):
        """avro schema
        use provided schema if available.
        """
        if self.schema is not None:
            return make_avro_schema(self.schema), the_iterable
        else:
            return infer_avro_schema(the_iterable)

    def process(self, the_iterator):
        """
        controlling proces loop

        fixme: add support for stats
        """
        stats = self.stats.start()
        if self.writer.is_open:
            out_stream = self.writer.stream
            self.__write(out_stream, the_iterator)
        else:
            with self.writer.open() as out_stream:
                self.__write(out_stream, the_iterator)
        stats.stop()
        return self


class AvroSource(source.AbstractMultiReaderSource):
    """
    read records from an Avro source

    :reader_list: list of reader objects
    :field_names: (optional) list of fields to extract
    """
    def iter_some_fields(self, field_names):
        """
        called when specific fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for record in avro_reader(o_reader):
                    yield({k: v for k, v in record.items() if k in field_names})
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for record in avro_reader(in_file):
                        yield({k: v for k, v in record.items() if k in field_names})
                        stats.increment()
        stats.stop()

    def iter_all_fields(self):
        """
        called when all fields specified
        """
        stats = self.stats.start()
        for o_reader in self.reader_list:
            if o_reader.is_open:
                for record in avro_reader(o_reader):
                    yield record
                    stats.increment()
            else:
                with o_reader.open() as in_file:
                    for record in avro_reader(in_file):
                        yield record
                        stats.increment()
        stats.stop()
