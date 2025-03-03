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
Oct 2023    Cobus Nel       clear_partitions
                            write_dataset
=========== =============== =================================================
"""
import logging
from itertools import islice, chain
from os import path
from typing import Dict, List

import pyarrow as pa
from jinja2 import Template
from pyarrow.fs import FileSystem, LocalFileSystem, S3FileSystem

from .. import source, sink
from ... import CHUNK_SIZE, messages
from ...data.iteration import chunker
from ...utilities.cmd_helper import LazyLoad
from ..model import Entity, ETLServices


# pa = LazyLoad("pyarrow")
# import pyarrow.parquet as pq
pq = LazyLoad("pyarrow.parquet")

logger = logging.getLogger("ext_arrow")


__all__ = []

# convert cannonical to arrow


class ArrowServices(ETLServices):

    def get_s3_fs(self, secret_name: str):
        """instantiate Arrow S3 instance"""
        secret = self.model.get_secret(secret_name)
        region = secret.parameters.get("region", None)
        if region is None:
            logger.info("No region specified for S3 data source")
        return S3FileSystem(
            access_key=secret.key,
            secret_key=secret.secret,
            region=region
        )

    def get_arrow_schema(self, entity_name):
        """
        load arrow schema from model entity
        """
        schema = self.model.entities[entity_name]
        return make_arrow_schema(schema)


def make_decimal(t=None):
    """create decimal value"""
    if not t:
        t = {
            "precision": 12,
            "scale": 2,
        }
    precision = t.get("precision", 12)
    scale = t.get("scale", 2)
    if precision < 38:
        return pa.decimal128(precision, scale)
    else:
        return pa.decimal256(precision, scale)


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


def infer_arrow_schema(iterable, n=50, enforce=True):
    """
    infer schema from iterable
    args:
        * iterable: data input
        * n: number of samples to infer schema
        * enforce: enforce schema if True

    returns:
        * arrow schema
        * a reconstructed iterable
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    entity = Entity.from_iterable(buffer, p=1.0, k=n)
    schema = make_arrow_schema(entity)
    if enforce is True:
        return schema, entity(chain(buffer, i))
    else:
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


def auto_write_parquet(path: str, iterable, n=100):
    """
    infer schema and write to parquet file

    args:
        - path: file path
        - iterable: iterable of dicts
        - n: number of records used to infer schema
    """
    schema, data = infer_arrow_schema(iterable, n)
    table = build_table(data, schema)
    write_parquet_file(table, path)


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
    logger.info(f"writing parquet to path {path}")
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
        existing_data_behavior=existing_data_behaviour,
        filesystem=fs,
        compression=compression
        # basename_template="chunk.{i}.snappy.parquet"
    )
    logger.debug("write completed")


def make_partition_path(partition_cols: List[str], partition_map: Dict,
                        base_path: str = None) -> str:
    """
    calculate partition path based on keys and values

    Arguments:
        * partition_cols: list of columns used for partitioning. e.g
    """
    for k in partition_cols:
        if k not in partition_map:
            raise KeyError(messages.MSH_0028.format(k))
    subdir = '/'.join(
        [
            '{colname}={value}'.format(colname=name, value=val)
            for name, val in partition_map.items()
        ]
    )
    if base_path is not None:
        retval = path.join(base_path, subdir)
    else:
        retval = subdir
    if "=" not in retval:
        raise ValueError(messages.MSH_0029)
    return retval


def clear_partition_data(f_system: FileSystem, partition_cols: List[str],
                         partition_map: Dict, base_path: str = None):
    """
    Clear data for partition specified

    Parameters
    ----------
    f_system: filesystem instance
        FileSystem instance.  Must be a pyarrow.fs type
        if None will use LocalFilesystem
    partition_cols: List
        list of partition columns e.b. ["month_id", "day_id"].  Required
        to make sure that all partitions is specified
    partition_map: Dict
        dict containing keys an values
    base_path: str
        filesystem base path. e.g. 's3://bucket/folder'


    Called as::

        from pyarrow.fs import LocalFileSystem

        fs = LocalFileSystem()
        pc = ["month_id", "day_id"]
        dm = {"month_id": 20231001, "day_id": 20231002}
        bp = "data/sales"
        clear_partition(fs, pc, dm, bp)

    Note: both partition_cols and partition_map is required to ensure
    data is not deleted accidentaly

    """
    fs = f_system if f_system else LocalFileSystem()
    p_path = make_partition_path(partition_cols, partition_map, base_path)
    logger.info(f"deleting files from {p_path}")
    try:
        fs.delete_dir_contents(p_path)
    except FileNotFoundError:
        logger.info(f"path {p_path} not found, ignoring clear operation")


def write_chunked_datasets(
    data, path, schema, partition_cols, fs=None,
    chunk_size=1_000_000, compression="snappy",
    existing_data_behaviour="overwrite_or_ignore"
):
    """
    Write chunks to parquet dataset

    Parameters:
    table: pyarrow Table instance
    path: str
        filesystem path
    fs: pyarrow.fs.FileSystem
        Filesystem instance (e.g. Arrow S3FileSystem)
    compression: str
        e.g. snappy
    existing_data_behaviour: str
        can be one of:
            - overwrite_or_ignore
            - error
            - delete_matching
    """
    for chunk in chunker(data, chunk_size):
        table = build_table(chunk, schema=schema)
        if len(table) > 0:
            # dont write an empty table
            write_parquet_dataset(
                table=table,
                path=path,
                partition_cols=partition_cols,
                fs=fs,
                compression=compression,
                existing_data_behaviour=existing_data_behaviour
            )
