from contextlib import contextmanager
import os
import pickle
from ..exceptions import DKitETLException, DKitArgumentException
from .. import messages
from . import (reader, source, sink, writer)
from .extensions import (
    ext_bxr,
    ext_msgpack,
    ext_sql_alchemy,
    ext_tables,
    ext_xlsx,
    ext_xls,
    ext_avro,
    ext_arrow
)

import gzip
import bz2
import lzma
import _pickle
from ..parsers import uri_parser

BINARY_DIALECTS = ["mpak", "pkl", "avro", "parquet"]

READER_MAP = {
    None: reader.FileReader,
    "bz2": reader.Bz2Reader,
    "xz": reader.LzmaReader,
    "gz": reader.GzipReader,
    "lz4": reader.Lz4Reader,
}

SOURCE_MAP = {
    "parquet": ext_arrow.ParquetSource,
    "avro": ext_avro.AvroSource,
    "bxr": ext_bxr.BXRSource,
    "csv": source.CsvDictSource,
    "json": source.JsonSource,
    "jsonl": source.JsonlSource,
    "mpak": ext_msgpack.MsgpackSource,
    "pke": source.EncryptSource,
    "pkl": source.PickleSource,
    "xls": ext_xls.XLSSource,
    "xlsx": ext_xlsx.XLSXSource,
}

SINK_MAP = {
    "parquet": ext_arrow.ParquetSink,
    "avro": ext_avro.AvroSink,
    "bxr": ext_bxr.BXRSink,
    "csv": sink.CsvDictSink,
    "json": sink.JsonSink,
    "jsonl": sink.JsonlSink,
    "mpak": ext_msgpack.MsgpackSink,
    "pke": sink.EncryptSink,
    "pkl": sink.PickleSink,
    "xlsx": ext_xlsx.XlsxSink,
}

WRITER_MAP = {
    None: writer.FileWriter,
    "bz2": writer.Bz2Writer,
    "gz": writer.GzipWriter,
    "xz": writer.LzmaWriter,
    "lz4": writer.Lz4Writer,
    }


COMPRESS_LIB = {
    "gz": gzip,
    "bz2": bz2,
    "xz": lzma,
}


class Dumper(object):
    """
    Simple class to dump and read pickle files.
    """
    def __init__(self, filename, pickler=pickle):
        self.filename = filename
        self.pickler = pickler

    def load(self):
        if os.path.exists(self.filename):
            with open(self.filename, "rb") as infile:
                return(self.pickler.load(infile))
        else:
            return None

    def dump(self, data):
        with open(self.filename, "wb") as outfile:
            self.pickler.dump(data, outfile)
        return data


def open_sink(uri: str, key=None) -> sink.AbstractSink:
    """
    parse uri string and open + return sink
    """
    return sink_factory(uri_parser.parse(uri), key)


def _sink_factory(uri_struct, key=None):
    """
    Instantiate a sink object from uri
    """
    cleanup = []

    def make_writer_instance(uri_struct):
        """instantiate and return"""
        database = uri_struct["database"]
        compression = uri_struct["compression"]
        if database == "stdio":
            return writer.StdOutWriter()
        elif uri_struct["dialect"] in BINARY_DIALECTS:
            w = WRITER_MAP[compression](database, mode="wb")
            # cleanup.append(w)
            return w
        else:
            compression = uri_struct["compression"]
            w = WRITER_MAP[compression](database)
            # cleanup.append(w)
            return w

    def make_file_sink(uri_struct):
        """
        instantiates file based sinks
        """
        snk = SINK_MAP[uri_struct["dialect"]]
        if uri_struct["dialect"] in ["xlsx", "xls"]:
            s = snk(uri_struct["database"])
            # cleanup.append(s)
            return s
        elif uri_struct["dialect"] in ["pke"]:
            if uri_struct["compression"]:
                compress = COMPRESS_LIB[uri_struct["compression"]]
            else:
                compress = None
            s = snk(
                uri_struct["database"],
                compress=compress,
                serde=_pickle,
                key=key,
            )
            return s
        else:
            s = snk(
                make_writer_instance(uri_struct),
            )
            # cleanup.append(s)
            return s

    def make_shm_sink(uri_struct):
        dialect = uri_struct["dialect"]
        if dialect not in ["pkl"]:
            raise DKitETLException("Shared Memory source only work with pickle")
        snk = SINK_MAP[dialect]
        _writer = writer.SharedMemoryWriter(
            file_name=uri_struct["database"],
            compression=uri_struct["compression"]
        )
        return snk(_writer)

    def make_hdf5_sink(uri_struct):
        file_name = uri_struct["database"]
        node = uri_struct["entity"]
        accessor = ext_tables.PyTablesAccessor(file_name)
        cleanup.append(accessor)
        return ext_tables.PyTablesSink(accessor, node)

    def make_sqla_sink(uri_struct):
        table_name = uri_struct["entity"]
        uri = ext_sql_alchemy.as_sqla_url(uri_struct)
        accessor = ext_sql_alchemy.SQLAlchemyAccessor(
            uri,
            echo=False
        )
        cleanup.append(accessor)
        return ext_sql_alchemy.SQLAlchemySink(
            accessor,
            table_name,
        )

    dispatcher = {
        "shm": make_shm_sink,
        "file": make_file_sink,
        "hdf5": make_hdf5_sink,
        "sqlite": make_sqla_sink,
        "mysql": make_sqla_sink,
        "mysql+mysqldb": make_sqla_sink,
        "mysql+mysqlconnector": make_sqla_sink,
        "postgresql": make_sqla_sink,
    }

    # Main logic
    disp = dispatcher[uri_struct["driver"]]
    return cleanup, disp(uri_struct)


@contextmanager
def sink_factory(uri_struct, key=None):
    """
    Instantiate a sink object from uri
    """
    cleanup, factory = _sink_factory(uri_struct, key=key)
    try:
        yield factory
    finally:
        for obj in cleanup:
            obj.close()


@contextmanager
def open_source(uri: str, skip_lines=0, field_names=None, delimiter=",",
                headings=None, key=None):
    """parse uri string and open + return sink"""
    try:
        parsed = uri_parser.parse(uri)
        factory = _SourceIterFactory(
            parsed, skip_lines, field_names, delimiter, headings, key=key
        )
        yield factory
    finally:
        factory.close()


@contextmanager
def source_factory(file_list, skip_lines=0, field_names=None, delimiter=",",
                   headings=None, where_clause=None, key=None):
    """
    Instantiates Source objects from a list of uri's

    Arguments:
        uri_list: list of uri strings
        kind: type of sink (auto, xlsx, csv, jsonl)
        skip_lines: (optional) number of lines to skip
        field_names: (optional) list of field names to extract
        delimiter: (optional) csv delimiter
        headings: (optional) csv headings if not in file
        key: (optional) encryption key
    """
    try:
        factory = _SourceIterFactory(
            file_list, skip_lines, field_names, delimiter, where_clause, headings,
            key=key
        )
        yield factory
    finally:
        factory.close()


class _SourceIterFactory(object):
    """
    Instantiates Source objects from a list of uri's

    Arguments:
        uri_list: list of uri strings
        skip_lines: (optional) number of lines to skip
        field_names: (optional) list of field names to extract
        delimiter: (optional) csv delimiter
    """
    def __init__(self, uri_struct, skip_lines=0, field_names=None, delimiter=",",
                 where_clause=None, headings=None, key=None, work_sheet=None):
        self.uri_struct = uri_struct
        self.skip_lines = skip_lines
        self.field_names = field_names
        self.delimiter = delimiter
        self.cleanup = []
        self.where_clause = where_clause
        self.headings = headings
        self.key = key
        self.work_sheet = work_sheet  # For xlsx

    def __make_source(self, uri_struct):
        """
        def get source for one file.
        """
        dispatcher = {
            "shm": self.__make_shm_source,
            "file": self.__make_file_source,
            "hdf5": self.__make_hdf5_source,
            "sqlite": self.__make_sqla_source,
            "mysql+mysqlconnector": self.__make_sqla_source,
            "mysql": self.__make_sqla_source,
            "mysql+mysqldb": self.__make_sqla_source,
            "postgres": self.__make_sqla_source,
        }
        return dispatcher[uri_struct["driver"]](uri_struct)

    def __make_hdf5_source(self, uri_struct):
        """instantiate an hdf5 source"""
        accessor = ext_tables.PyTablesAccessor(uri_struct["database"], mode="r")
        full_path = uri_struct["entity"]
        # where_clause = self.where_clause if self.where_clause else uri_struct["filter"]
        self.cleanup.append(accessor)
        return ext_tables.PyTablesSource(
            accessor,
            full_path,
            # where_clause,
            field_names=self.field_names,
        )

    def __make_sqla_source(self, uri_struct):
        """instantiate sqlite source"""
        uri = ext_sql_alchemy.as_sqla_url(uri_struct)
        accessor = ext_sql_alchemy.SQLAlchemyAccessor(uri, echo=False)
        self.cleanup.append(accessor)
        return ext_sql_alchemy.SQLAlchemyTableSource(
            accessor,
            uri_struct["entity"],
            # where_clause=self.where_clause if self.where_clause else uri_struct["filter"],
            field_names=self.field_names,
        )

    def __make_shm_source(self, uri_struct):
        """make a shared memory reader"""
        the_source = SOURCE_MAP[uri_struct["dialect"]]
        the_reader = reader.SharedMemoryReader(
            uri_struct["database"],
            compression=uri_struct["compression"]
        )
        src = the_source(
            [the_reader],
            field_names=self.field_names,
        )
        return src

    def __make_file_source(self, uri_struct):
        """make a file based reader"""

        the_source = SOURCE_MAP[uri_struct["dialect"]]

        # Excel Only
        if uri_struct["dialect"] in ["xlsx", "xls"]:
            src = the_source(
                [uri_struct["database"]],
                field_names=self.field_names,
                skip_lines=self.skip_lines,
                work_sheet=self.work_sheet
            )
            self.cleanup.append(src)
            return src

        # CSV Only
        elif uri_struct["dialect"] in ["csv"]:
            if uri_struct["database"] == "stdio":
                src = the_source(
                    [reader.StdinReader()],
                    field_names=self.field_names,
                    delimiter=self.delimiter,
                    skip_lines=self.skip_lines,
                    headings=self.headings
                )
                self.cleanup.append(src)
                return src
            else:
                the_reader = READER_MAP[uri_struct["compression"]]
                src = the_source(
                    [the_reader(uri_struct["database"])],
                    field_names=self.field_names,
                    delimiter=self.delimiter,
                    skip_lines=self.skip_lines,
                    headings=self.headings
                )
                self.cleanup.append(src)
                return src

        elif uri_struct["dialect"] in ["pke"]:
            # Encryption key must be specified
            if not self.key:
                raise DKitArgumentException(messages.MSG_0022)

            if uri_struct["compression"]:
                compression = COMPRESS_LIB[uri_struct["compression"]]
            else:
                compression = None

            return the_source(
                uri_struct["database"],
                compression=compression,
                key=self.key,
                headings=self.headings,
                field_names=self.field_names,
            )

        # All others
        else:
            if uri_struct["database"] == "stdio":
                return the_source(
                    [reader.StdinReader()],
                    field_names=self.field_names,
                )
            else:
                the_reader = READER_MAP[uri_struct["compression"]]
                if (uri_struct["dialect"] in BINARY_DIALECTS):
                    return the_source(
                        [the_reader(uri_struct["database"], mode="rb")],
                        field_names=self.field_names,
                    )
                else:
                    return the_source(
                        [the_reader(uri_struct["database"])],
                        field_names=self.field_names,
                    )

    def close(self):
        for obj in self.cleanup:
            obj.close()

    def __iter__(self):
        yield from self.__make_source(self.uri_struct)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
