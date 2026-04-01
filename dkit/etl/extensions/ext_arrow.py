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
PyArrow schema, parquet, and dataset helpers for dkit ETL pipelines.

=========== =============== =================================================
Jul 2019    Cobus Nel       Initial version
Sep 2022    Cobus Nel       Added:
                            - schema create tools
                            - Parquet source and sinks
May 2023    Cobus Nel       Added Unsigned int types
Oct 2023    Cobus Nel       clear_partitions
                            write_dataset
Feb 2025    Cobus Nel       Added ArrowServices
Mar 2026    Cobus Nel       Refactor:
                            - typing
                            - tests
                            - logic errors
=========== =============== =================================================
"""
import logging
import textwrap
from collections.abc import Iterator, Mapping, Sequence
from itertools import islice, chain
from os import path
from typing import Any

import pyarrow as pa
from jinja2 import Template
from pyarrow.fs import FileSystem, LocalFileSystem, S3FileSystem, AwsDefaultS3RetryStrategy

from .. import source, sink
from ... import CHUNK_SIZE, messages
from ...data.iteration import chunker
from ...typing_helper import FieldDefinition, Row, RowIterable
from ...utilities.cmd_helper import LazyLoad
from ..model import Entity, ETLServices


# pa = LazyLoad("pyarrow")
# import pyarrow.parquet as pq
pq = LazyLoad("pyarrow.parquet")

logger = logging.getLogger("ext_arrow")


__all__ = [
    "ArrowSchemaGenerator",
    "ArrowServices",
    "ParquetSink",
    "ParquetSource",
    "auto_write_parquet",
    "build_table",
    "clear_partition_data",
    "infer_and_coerce_arrow_schema",
    "infer_arrow_schema",
    "make_arrow_schema",
    "make_partition_path",
    "write_chunked_datasets",
    "write_parquet_dataset",
    "write_parquet_file",
]


def make_decimal(t: FieldDefinition | None = None) -> pa.DataType:
    """
    Build a PyArrow decimal type from a canonical decimal definition.

    Args:
        t: Canonical decimal field definition containing optional
            ``precision`` and ``scale`` keys.

    Returns:
        A ``pyarrow.Decimal128Type`` or ``pyarrow.Decimal256Type``.
    """
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


def render_decimal_type(definition: FieldDefinition) -> str:
    """
    Render Python source for a PyArrow decimal type constructor.

    Args:
        definition: Canonical decimal field definition.

    Returns:
        Python source code for the matching PyArrow decimal constructor.
    """
    precision = definition.get("precision", 12)
    scale = definition.get("scale", 2)
    if precision < 38:
        return f"pa.decimal128({precision}, {scale})"
    return f"pa.decimal256({precision}, {scale})"


ARROW_TYPE_SPECS = {
    "float": {
        "runtime": lambda t: pa.float64(),
        "render": lambda t: "pa.float64()",
    },
    "double": {
        "runtime": lambda t: pa.float64(),
        "render": lambda t: "pa.float64()",
    },
    "integer": {
        "runtime": lambda t: pa.int32(),
        "render": lambda t: "pa.int32()",
    },
    "int8": {
        "runtime": lambda t: pa.int8(),
        "render": lambda t: "pa.int8()",
    },
    "int16": {
        "runtime": lambda t: pa.int16(),
        "render": lambda t: "pa.int16()",
    },
    "int32": {
        "runtime": lambda t: pa.int32(),
        "render": lambda t: "pa.int32()",
    },
    "int64": {
        "runtime": lambda t: pa.int64(),
        "render": lambda t: "pa.int64()",
    },
    "uint8": {
        "runtime": lambda t: pa.uint8(),
        "render": lambda t: "pa.uint8()",
    },
    "uint16": {
        "runtime": lambda t: pa.uint16(),
        "render": lambda t: "pa.uint16()",
    },
    "uint32": {
        "runtime": lambda t: pa.uint32(),
        "render": lambda t: "pa.uint32()",
    },
    "uint64": {
        "runtime": lambda t: pa.uint64(),
        "render": lambda t: "pa.uint64()",
    },
    "string": {
        "runtime": lambda t: pa.string(),
        "render": lambda t: "pa.string()",
    },
    "boolean": {
        "runtime": lambda t: pa.bool_(),
        "render": lambda t: "pa.bool_()",
    },
    "binary": {
        "runtime": lambda t: pa.binary(),
        "render": lambda t: "pa.binary()",
    },
    "datetime": {
        "runtime": lambda t: pa.timestamp("us"),
        "render": lambda t: 'pa.timestamp("us")',
    },
    "date": {
        "runtime": lambda t: pa.date32(),
        "render": lambda t: "pa.date32()",
    },
    "decimal": {
        "runtime": make_decimal,
        "render": render_decimal_type,
    },
}


class ArrowServices(ETLServices):

    def get_s3_fs(self, secret_name: str) -> S3FileSystem:
        """
        Create a PyArrow S3 filesystem from a named model secret.

        Args:
            secret_name: Name of the secret in the loaded model.

        Returns:
            A configured ``pyarrow.fs.S3FileSystem`` instance.
        """
        secret = self.model.get_secret(secret_name)
        region = secret.parameters.get("region", None)
        if region is None:
            logger.info("No region specified for S3 data source")
        return S3FileSystem(
            access_key=secret.key,
            secret_key=secret.secret,
            region=region,
            retry_strategy=AwsDefaultS3RetryStrategy(max_attempts=5)
        )

    def get_arrow_schema(self, entity_name) -> pa.Schema:
        """
        Load and convert a model entity to a PyArrow schema.

        Args:
            entity_name: Name of the entity in the loaded model.

        Returns:
            The entity converted to ``pyarrow.Schema``.
        """
        schema = self.model.entities[entity_name]
        return make_arrow_schema(schema)


str_template = textwrap.dedent("""\
import pyarrow as pa
{% for entity, typemap in entities.items() %}

# {{ entity }}
schema_{{ entity }} = pa.schema(
    [
{%- for field, props in typemap.schema.items() %}
{%- set nullable = ", " + str(props["nullable"]) if "nullable" in props else "" %}
        pa.field("{{ field }}", {{ render_type(props) }}{{ nullable }}),
{%- endfor %}
    ]
)
{% endfor %}
entity_map = {
{%- for entity in entities.keys() %}
    "{{ entity }}": schema_{{ entity }},
{%- endfor %}
}
""")


def _render_arrow_type(definition: FieldDefinition):
    """
    Render Python source for the PyArrow type of one canonical field.

    Args:
        definition: Canonical field definition.

    Returns:
        Python source code for the matching PyArrow type constructor.
    """
    type_name = definition["type"]
    return ARROW_TYPE_SPECS[type_name]["render"](definition)


def make_arrow_schema(cannonical_schema: Entity) -> pa.Schema:
    """
    Convert a canonical ``Entity`` schema to a ``pyarrow.Schema``.

    Args:
        cannonical_schema: Canonical entity definition.

    Returns:
        A ``pyarrow.Schema`` built from the entity fields.
    """
    fields = []
    validator = cannonical_schema.as_entity_validator()
    for name, definition in validator.schema.items():
        fields.append(
            pa.field(
                name,
                ARROW_TYPE_SPECS[definition["type"]]["runtime"](definition)
            )
        )
    return pa.schema(fields)


class ArrowSchemaGenerator(object):
    """
    Generate Python source that declares PyArrow schemas for entities.
    """

    def __init__(self, **entities):
        self.__entities = entities

    @property
    def entities(self):
        """
        Return the entity map used for schema generation.

        Returns:
            A mapping of entity names to entity validators or schemas.
        """
        return self.__entities

    def create_schema(self):
        """
        Render Python source that defines the configured Arrow schemas.

        Returns:
            Python source code containing schema declarations and an entity map.
        """
        template = Template(str_template)
        return template.render(
            entities=self.entities,
            render_type=_render_arrow_type,
            str=str
        )


def infer_arrow_schema(iterable: RowIterable, n: int = 50) -> tuple[pa.Schema, Iterator[Row]]:
    """
    Infer an Arrow schema from sampled rows without coercing row values.

    Args:
        iterable: Input row iterator.
        n: Number of rows to sample for inference.

    Returns:
        A tuple of ``(schema, iterable)``, where ``schema`` is the inferred
        ``pyarrow.Schema`` and the returned iterable yields the sampled rows
        followed by the remaining input rows unchanged.
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    entity = Entity.from_iterable(
        buffer,
        infer_strings=False,
        strict_numbers=True,
        p=1.0,
        k=n
    )
    schema = make_arrow_schema(entity)
    return schema, chain(buffer, i)


def infer_and_coerce_arrow_schema(
    iterable: RowIterable, n: int = 50
) -> tuple[pa.Schema, Iterator[Row]]:
    """
    Infer an Arrow schema and coerce row values to the inferred entity types.

    Args:
        iterable: Input row iterator.
        n: Number of rows to sample for inference.

    Returns:
        A tuple of ``(schema, iterable)``, where ``schema`` is the inferred
        ``pyarrow.Schema`` and the returned iterable yields rows coerced to the
        inferred canonical entity.
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    entity = Entity.from_iterable(buffer, infer_strings=True, p=1.0, k=n)
    schema = make_arrow_schema(entity)
    return schema, entity(chain(buffer, i))


def build_table(data: RowIterable, schema=None, micro_batch_size=CHUNK_SIZE) -> pa.Table:
    """
    Build a ``pyarrow.Table`` from row dictionaries in micro-batches.

    Args:
        data: Iterable of row dictionaries.
        schema: Optional Arrow schema to enforce while building batches.
        micro_batch_size: Number of rows to convert per record batch.

    Returns:
        A ``pyarrow.Table`` assembled from record batches.
    """

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
    Read parquet sources and yield row dictionaries via Arrow record batches.

    Args:
        reader_list: Reader objects that provide parquet input streams.
        field_names: Optional field names to project from the parquet inputs.
        chunk_size: Number of rows per Arrow batch.
    """
    def __init__(
        self,
        reader_list,
        field_names: Sequence[str] | None = None,
        chunk_size: int = CHUNK_SIZE,
    ):
        super().__init__(reader_list, field_names)
        self.chunk_size = chunk_size

    def iter_some_fields(self, field_names):
        """
        Yield dictionary rows from parquet files for selected columns only.

        Args:
            field_names: Requested field names. The implementation uses
                ``self.field_names`` configured on the source instance.

        Yields:
            Row dictionaries containing only the selected columns.
        """
        self.stats.start()

        for o_reader in self.reader_list:

            if o_reader.is_open:
                parq_file = pq.ParquetFile(o_reader)
                for batch in parq_file.iter_batches(
                    self.chunk_size, columns=self.field_names
                ):
                    yield from batch.to_pylist()
                    self.stats.increment(len(batch))
            else:
                with o_reader.open() as in_file:
                    parq_file = pq.ParquetFile(in_file)
                    for batch in parq_file.iter_batches(
                        self.chunk_size, columns=self.field_names
                    ):
                        yield from batch.to_pylist()
                        self.stats.increment(len(batch))

        self.stats.stop()

    def iter_all_fields(self):
        """
        Yield dictionary rows from parquet files for all columns.

        Yields:
            Row dictionaries for every column in the parquet inputs.
        """
        self.stats.start()

        for o_reader in self.reader_list:

            if o_reader.is_open:
                parq_file = pq.ParquetFile(o_reader)
                for batch in parq_file.iter_batches(self.chunk_size):
                    yield from batch.to_pylist()
                    self.stats.increment(len(batch))
            else:
                with o_reader.open() as in_file:
                    parq_file = pq.ParquetFile(in_file)
                    for batch in parq_file.iter_batches(self.chunk_size):
                        yield from batch.to_pylist()
                        self.stats.increment(len(batch))

        self.stats.stop()


class ParquetSink(sink.AbstractSink):
    """
    Write dictionary rows to parquet using PyArrow.

    When ``schema`` is omitted, the sink infers an Arrow schema from sampled
    rows. When ``coerce=True``, rows are coerced to the inferred or supplied
    canonical entity before conversion to Arrow record batches.

    Args:
        writer: Output writer object.
        field_names: Unsupported field selection argument.
        schema: Optional canonical ``Entity`` schema.
        chunk_size: Number of rows to process per output batch.
        compression: Parquet compression codec.
        coerce: Coerce rows to the inferred or supplied schema before writing.
    """
    def __init__(
        self,
        writer,
        field_names: Sequence[str] | None = None,
        schema: Entity | None = None,
        chunk_size: int = 50_000,
        compression: str = "snappy",
        coerce: bool = False,
    ):
        super().__init__()
        self.writer = writer
        self.chunk_size = chunk_size
        self.schema = schema
        self.compression = compression
        self.coerce = coerce
        if field_names is not None:
            raise NotImplementedError("field_names not implemented")

    def __write_all(self, writer, the_iterator: RowIterable):
        """
        Write all rows from an iterator to an open parquet output.

        Args:
            writer: Open output stream or writer handle.
            the_iterator: Iterable of row dictionaries.
        """
        table = self.__build_table(the_iterator, self.schema, self.chunk_size)
        pq.write_table(
            table,
            writer,
            compression=self.compression,
        )

    def __build_table(
        self,
        data: RowIterable,
        schema: Entity | None,
        micro_batch_size: int,
    ) -> pa.Table:
        """
        Build a table for parquet output and update sink row statistics.

        Args:
            data: Iterable of row dictionaries.
            schema: Optional canonical ``Entity`` schema.
            micro_batch_size: Number of rows to convert per record batch.

        Returns:
            A ``pyarrow.Table`` ready to be written to parquet.
        """
        _data = data
        if schema is None:
            logger.info("No schema provided, generating arrow schema from data")
            if self.coerce is True:
                _schema, _data = infer_and_coerce_arrow_schema(data, 1_000)
            else:
                _schema, _data = infer_arrow_schema(data, 1_000)
        else:
            _schema = make_arrow_schema(schema)
            if self.coerce is True:
                _data = schema(_data)

        def iter_batch():
            for chunk in chunker(_data, size=micro_batch_size):
                rows = list(chunk)
                yield pa.RecordBatch.from_pylist(
                    rows,
                    schema=_schema
                )
                self.stats.increment(len(rows))

        return pa.Table.from_batches(
            iter_batch()
        )

    def process(self, the_iterator: RowIterable):
        """
        Write rows from an iterator to parquet.

        Args:
            the_iterator: Iterable of row dictionaries.

        Returns:
            The sink instance.
        """
        self.stats.start()
        if self.writer.is_open:
            self.__write_all(self.writer, the_iterator)
        else:
            with self.writer.open() as out_stream:
                self.__write_all(out_stream, the_iterator)
        self.stats.stop()
        return self


def auto_write_parquet(path: str, iterable: RowIterable, n: int = 100, coerce: bool = False):
    """
    Infer a schema and write dictionary rows to a parquet file.

    Args:
        path: Output parquet file path.
        iterable: Iterable of row dictionaries.
        n: Number of records used for schema inference.
        coerce: Coerce row values to the inferred schema when ``True``.

    Returns:
        ``None``.
    """
    if coerce is True:
        schema, data = infer_and_coerce_arrow_schema(iterable, n)
    else:
        schema, data = infer_arrow_schema(iterable, n)
    table = build_table(data, schema)
    write_parquet_file(table, path)


def write_parquet_file(
    table: pa.Table,
    path: str,
    fs: FileSystem | None = None,
    compression: str = "snappy",
):
    """
    Write a ``pyarrow.Table`` to a parquet file.

    Args:
        table: Arrow table instance.
        path: Output filesystem path.
        fs: Optional Arrow filesystem instance.
        compression: Parquet compression codec.

    Returns:
        ``None``.
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
    table: pa.Table,
    path: str,
    partition_cols: Sequence[str],
    fs: FileSystem | None = None,
    compression: str = "snappy",
    existing_data_behaviour: str = "overwrite_or_ignore",
):
    """
    Write a partitioned parquet dataset from an Arrow table.

    Args:
        table: Arrow table instance.
        path: Root filesystem path for the dataset.
        partition_cols: Column names used to partition the dataset.
        fs: Optional Arrow filesystem instance.
        compression: Parquet compression codec.
        existing_data_behaviour: Behaviour when data already exists. Supported
            values are ``overwrite_or_ignore``, ``error``, and
            ``delete_matching``.

    Returns:
        ``None``.
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


def make_partition_path(
    partition_cols: Sequence[str],
    partition_map: Mapping[str, Any],
    base_path: str | None = None,
) -> str:
    """
    Build a partition path from partition column names and values.

    Args:
        partition_cols: Ordered list of partition column names.
        partition_map: Mapping of partition column names to values.
        base_path: Base filesystem path or URI prefix.

    Returns:
        The full partition path.

    Raises:
        ValueError: If ``base_path`` is ``None`` or the result is invalid.
        KeyError: If a required partition column is missing from
            ``partition_map``.
    """
    if base_path is None:
        raise ValueError("parameter base_path cannot be None'")
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


def clear_partition_data(
    f_system: FileSystem | None,
    partition_cols: Sequence[str],
    partition_map: Mapping[str, Any],
    base_path: str | None = None,
):
    """
    Clear the contents of one partition directory.

    Args:
        f_system: Arrow filesystem instance. When ``None``, a local filesystem
            is used.
        partition_cols: Ordered list of required partition columns.
        partition_map: Mapping of partition column names to values.
        base_path: Base filesystem path or URI prefix.

    Example:

        from pyarrow.fs import LocalFileSystem

        fs = LocalFileSystem()
        pc = ["month_id", "day_id"]
        dm = {"month_id": 20231001, "day_id": 20231002}
        bp = "data/sales"
        clear_partition(fs, pc, dm, bp)

    Returns:
        ``None``.

    Note:
        Both ``partition_cols`` and ``partition_map`` are required so that
        partition deletion remains explicit and safe.
    """
    fs = f_system if f_system else LocalFileSystem()
    p_path = make_partition_path(partition_cols, partition_map, base_path)
    logger.info(f"deleting files from {p_path}")
    try:
        fs.delete_dir_contents(p_path)
    except FileNotFoundError:
        logger.info(f"path {p_path} not found, ignoring clear operation")


def write_chunked_datasets(
    data: RowIterable,
    path: str,
    schema: pa.Schema,
    partition_cols: Sequence[str],
    fs: FileSystem | None = None,
    chunk_size: int = 1_000_000,
    compression: str = "snappy",
    existing_data_behaviour: str = "overwrite_or_ignore",
):
    """
    Write iterable row data to a partitioned parquet dataset in chunks.

    Args:
        data: Iterable of row dictionaries.
        path: Root filesystem path for the dataset.
        schema: Arrow schema used when building each chunk table.
        partition_cols: Column names used for dataset partitioning.
        fs: Optional Arrow filesystem instance.
        chunk_size: Number of rows per chunk.
        compression: Parquet compression codec.
        existing_data_behaviour: Behaviour when data already exists. Supported
            values are ``overwrite_or_ignore``, ``error``, and
            ``delete_matching``.

    Returns:
        ``None``.
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
