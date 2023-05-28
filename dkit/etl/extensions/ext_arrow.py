# Copyright (c) 2019 Cobus Nel
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
Extension for pyarrow

=========== =============== =================================================
July 2019   Cobus Nel       Initial version
Sept 2022   Cobus Nel       Added:
                            - schema create tools
                            - Parquet source and sinks
May 2023    Cobus Nel       Added Unsigned int types
=========== =============== =================================================
"""
from ... import CHUNK_SIZE
from ...utilities.cmd_helper import LazyLoad
from .. import source, sink
from ...data.iteration import chunker
from ..model import Entity
from itertools import islice, chain
from jinja2 import Template
import logging


# pa = LazyLoad("pyarrow")
import pyarrow as pa
# import pyarrow.parquet as pq
pq = LazyLoad("pyarrow.parquet")

logger = logging.getLogger("ext_arrow")


__all__ = []

# convert cannonical to arrow


def make_decimal(t=None):
    """create decimal value"""
    if not t:
        t = {
            "precision": 10,
            "scale": 2,
        }
    return pa.decimal128(t["precision"], t["scale"])


ARROW_TYPEMAP = {
    "float": lambda t: pa.float32(),
    "double": lambda t: pa.float64(),
    "integer": lambda t: pa.int32(),
    "int8": lambda t: pa.int16(),    # int8 not available
    "int16": lambda t: pa.int16(),
    "int32": lambda t: pa.int32(),
    "int64": lambda t: pa.int64(),
    "uint8": lambda t: pa.uint16(),  # int8 not available
    "uint16": lambda t: pa.uint16(),
    "uint32": lambda t: pa.uint32(),
    "uint64": lambda t: pa.uint64(),
    "string": lambda t: pa.string(),
    "boolean": lambda t: pa.bool_(),
    "binary": lambda t: pa.binary(),
    # "datetime":  pa.time32("s"),
    "datetime": lambda t: pa.timestamp("s"),
    "date": lambda t: pa.date32(),
    "decimal": make_decimal,
}


str_template = """
import pyarrow as pa

{% for entity, typemap in entities.items() %}

# {{ entity }}
schema_{{ entity }} = pa.schema(
    [
        {% for field, props in typemap.schema.items() -%}
          {% if "nullable" in props -%}
            {% set nullable = ", " + str(props["nullable"]) -%}
          {% else -%}
            {% set nullable = "" -%}
        {% endif -%}
        pa.field("{{ field }}", pa.{{ tm[props["type"]] }}(){{ nullable }}),
        {% endfor -%}
    ]
)
{%- endfor %}

entity_map = {
{%- for entity in entities.keys() %}
    "{{ entity }}": schema_{{ entity }},
{%- endfor %}
}
"""


def make_arrow_schema(cannonical_schema: Entity):
    """create an Arrow schema from cannonical Entity"""
    fields = []
    validator = cannonical_schema.as_entity_validator()
    for name, definition in validator.schema.items():
        fields.append(
            pa.field(
                name,
                ARROW_TYPEMAP[definition["type"]](definition)
            )
        )
    return pa.schema(fields)


class ArrowSchemaGenerator(object):
    """
    Create .py file that define pyarrow schema fromm
    cannonical entity schema (s).
    """

    def __init__(self, **entities):
        self.__entities = entities
        self.type_map = {
            k: self.str_name(v)
            for k, v in
            ARROW_TYPEMAP.items()
        }

    @staticmethod
    def str_name(obj):
        """appropriate string name for pyarrow object"""
        sn = str(obj)
        if sn == "float":
            sn = f"{sn}{obj.bit_width}"
        return sn

    @property
    def entities(self):
        """
        dictionary of entities
        """
        return self.__entities

    def create_schema(self):
        """
        Create python code to define pyarrow schema
        """
        template = Template(str_template)
        return template.render(
            entities=self.entities,
            tm=self.type_map,
            str=str
        )


def infer_arrow_schema(iterable, n=50):
    """
    infer schema from iterable

    returns:
        * arrow schema
        * a reconstructed iterable
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    schema = make_arrow_schema(
        Entity.from_iterable(buffer, p=1.0, k=n)
    )
    return schema, chain(buffer, i)


def build_table(data, schema=None, micro_batch_size=CHUNK_SIZE) -> pa.Table:
    """build pyarrow table"""

    def iter_batch():
        for chunk in chunker(data, size=micro_batch_size):
            yield pa.RecordBatch.from_pylist(
                list(chunk),
                schema=schema
            )

    return pa.Table.from_batches(
        iter_batch()
    )


class ParquetSource(source.AbstractMultiReaderSource):
    """
    Read parquet sources and convert to dict record format
    using the pyarrow library with record batches.

    Parameters:
        - reader_list: list of reader objects
        - field_names: extract only these fields
        - chunk_size: number of rows per batch (default 64K)

    """
    def __init__(self, reader_list, field_names=None, chunk_size=CHUNK_SIZE):
        super().__init__(reader_list, field_names)
        self.chunk_size = chunk_size

    def iter_some_fields(self, field_names):
        """convert parquet to dict records and yield per record

        only return required records
        """
        self.stats.start()

        for o_reader in self.reader_list:

            if o_reader.is_open:
                parq_file = pq.ParquetFile(o_reader)
                for batch in parq_file.iter_batches(
                    self.chunk_size, columns=self.field_names
                ):
                    yield from batch.to_pylist()
                    self.stats.increment(self.chunk_size)
            else:
                with o_reader.open() as in_file:
                    parq_file = pq.ParquetFile(in_file)
                    for batch in parq_file.iter_batches(
                        self.chunk_size, columns=self.field_names
                    ):
                        yield from batch.to_pylist()
                        self.stats.increment(self.chunk_size)

        self.stats.stop()

    def iter_all_fields(self):
        """convert parquet to dict records and yield per record"""
        self.stats.start()

        for o_reader in self.reader_list:

            if o_reader.is_open:
                parq_file = pq.ParquetFile(o_reader)
                for batch in parq_file.iter_batches(self.chunk_size):
                    yield from batch.to_pylist()
                    self.stats.increment(self.chunk_size)
            else:
                with o_reader.open() as in_file:
                    parq_file = pq.ParquetFile(in_file)
                    for batch in parq_file.iter_batches(self.chunk_size):
                        yield from batch.to_pylist()
                        self.stats.increment(self.chunk_size)

        self.stats.stop()


class ParquetSink(sink.AbstractSink):
    """
    serialize dict records data to parquet

    using the pyarrow library
    """
    def __init__(self, writer, field_names=None, schema=None,
                 chunk_size=50_000, compression="snappy"):
        super().__init__()
        self.writer = writer
        self.chunk_size = chunk_size
        self.schema = schema
        self.compression = compression
        if field_names is not None:
            raise NotImplementedError("field_names not implemented")

    def __write_all(self, writer, the_iterator):
        """write data to parquet"""
        table = self.__build_table(the_iterator, self.schema, self.chunk_size)
        pq.write_table(
            table,
            writer,
            compression=self.compression,
        )

    def __build_table(self, data, schema, micro_batch_size) -> pa.Table:
        """ build pyarrow table """
        # the same code as the standalone build_table function
        # but add code for incrementing counters
        _data = data
        if schema is None:
            logger.info("No schema provided, generating arrow schema from data")
            _schema, _data = infer_arrow_schema(data, 1_000)
        else:
            _schema = make_arrow_schema(schema)

        def iter_batch():
            for chunk in chunker(_data, size=micro_batch_size):
                yield pa.RecordBatch.from_pylist(
                    list(chunk),
                    schema=_schema
                )
                self.stats.increment(micro_batch_size)

        return pa.Table.from_batches(
            iter_batch()
        )

    def process(self, the_iterator):
        self.stats.start()
        if self.writer.is_open:
            self.__write_all(self.writer, the_iterator)
        else:
            with self.writer.open() as out_stream:
                self.__write_all(out_stream, the_iterator)
        self.stats.stop()
        return self


def write_parquet_file(table, path, fs=None, compression="snappy"):
    """write pyarrow table to parquet file

    convenience function to write a table to parquet with
    sensible default options for ETL work.

    args:

        - table: arrow Table instance
        - path: filesystem path
        - fs: Filesystem instance (e.g. Arrow S3FileSystem)
        - compression: e.g. snappy

    """
    logger.info(f"writing table of size {len(table)} to parquet")
    logger.debug(f"writing to path {path}")
    pq.write_table(
        table,
        path,
        filesystem=fs,
        compression=compression
    )
    logger.debug("write completed")


def write_parquet_dataset(
    table, path, partition_cols, fs=None,
    compression="snappy", existing_data_behaviour="overwrite_or_ignore"
):
    """write pyarrow table to parquet

    convenience function to write a table to parquet with
    sensible default options for ETL work.

    args:

        - table: arrow Table instance
        - path: filesystem path
        - fs: Filesystem instance (e.g. Arrow S3FileSystem)
        - compression: e.g. snappy
        - existing_data_behaviour can be one of:
            - overwrite_or_ignore
            - error
            - delete_matching
    """
    logger.info(f"writing table of size {len(table)} to parquet")
    logger.debug(f"writing to path {path}")
    pq.write_to_dataset(
        table,
        root_path=path,
        partition_cols=partition_cols,
        existing_data_behavior="overwrite_or_ignore",
        filesystem=fs,
        compression=compression
        # basename_template="chunk.{i}.snappy.parquet"
    )
    logger.debug("write completed")
