from contextlib import contextmanager
import os
import pickle
from ..exceptions import CkitETLException
from . import (reader, source, sink, writer)
from .extensions import (
    ext_bxr,
    ext_msgpack,
    ext_sql_alchemy,
    ext_tables,
    ext_xlsx,
    ext_xls
)


from ..parsers import uri_parser

BINARY_DIALECTS = ["mpak", "pkl"]

READER_MAP = {
    None: reader.FileReader,
    "bz2": reader.Bz2Reader,
    "xz": reader.LzmaReader,
    "gz": reader.GzipReader,
    "lz4": reader.Lz4Reader,
}

SOURCE_MAP = {
    "bxr": ext_bxr.BXRSource,
    "csv": source.CsvDictSource,
    "json": source.JsonSource,
    "jsonl": source.JsonlSource,
    "mpak": ext_msgpack.MsgpackSource,
    "pkl": source.PickleSource,
    "xls": ext_xls.XLSSource,
    "xlsx": ext_xlsx.XLSXSource,
}

SINK_MAP = {
    "jsonl": sink.JsonlSink,
    "json": sink.JsonSink,
    "csv": sink.CsvDictSink,
    "xlsx": ext_xlsx.XlsxSink,
    "pkl": sink.PickleSink,
    "mpak": ext_msgpack.MsgpackSink,
    "bxr": ext_bxr.BXRSink,
}

WRITER_MAP = {
    None: writer.FileWriter,
    "bz2": writer.Bz2Writer,
    "gz": writer.GzipWriter,
    "xz": writer.LzmaWriter,
    "lz4": writer.Lz4Writer,
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


def open_sink(uri: str, logger=None) -> sink.Sink:
    """
    parse uri string and open + return sink
    """
    return sink_factory(uri_parser.parse(uri), logger)


def _sink_factory(uri_struct, logger=None):
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
            s = snk(
                uri_struct["database"],
                logger=logger
            )
            # cleanup.append(s)
            return s
        else:
            s = snk(
                make_writer_instance(uri_struct),
                logger=logger
            )
            # cleanup.append(s)
            return s

    def make_shm_sink(uri_struct):
        dialect = uri_struct["dialect"]
        if dialect not in ["pkl"]:
            raise CkitETLException("Shared Memory source only work with pickle")
        snk = SINK_MAP[dialect]
        _writer = writer.SharedMemoryWriter(
            file_name=uri_struct["database"],
            compression=uri_struct["compression"]
        )
        return snk(_writer, logger=logger)

    def make_hdf5_sink(uri_struct):
        file_name = uri_struct["database"]
        node = uri_struct["entity"]
        accessor = ext_tables.PyTablesAccessor(file_name)
        cleanup.append(accessor)
        return ext_tables.PyTablesSink(accessor, node, logger=logger)

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
            logger=logger
        )

    dispatcher = {
        "shm": make_shm_sink,
        "file": make_file_sink,
        "hdf5": make_hdf5_sink,
        "sqlite": make_sqla_sink,
        "mysql": make_sqla_sink,
        "mysql+mysqlconnector": make_sqla_sink,
        "postgres": make_sqla_sink,
    }

    # Main logic
    disp = dispatcher[uri_struct["driver"]]
    return cleanup, disp(uri_struct)


@contextmanager
def sink_factory(uri_struct, logger=None):
    """
    Instantiate a sink object from uri
    """
    cleanup, factory = _sink_factory(uri_struct)
    try:
        yield factory
    finally:
        for obj in cleanup:
            obj.close()


@contextmanager
def open_source(uri: str, skip_lines=0, field_names=None, logger=None, delimiter=","):
    """parse uri string and open + return sink"""
    try:
        parsed = uri_parser.parse(uri)
        factory = _SourceIterFactory(parsed, skip_lines, field_names, logger, delimiter)
        yield factory
    finally:
        factory.close()


@contextmanager
def source_factory(file_list, skip_lines=0, field_names=None, logger=None, delimiter=",",
                   where_clause=None):
    """
    Instantiates Source objects from a list of uri's

    Arguments:
        uri_list: list of uri strings
        kind: type of sink (auto, xlsx, csv, jsonl)
        skip_lines: (optional) number of lines to skip
        field_names: (optional) list of field names to extract
        logger: (optional) logging instance
        delimiter: (optional) csv delimiter
    """
    try:
        factory = _SourceIterFactory(
            file_list, skip_lines, field_names, logger, delimiter, where_clause
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
        logger: (optional) logging instance
        delimiter: (optional) csv delimiter
    """
    def __init__(self, uri_struct, skip_lines=0, field_names=None, logger=None,
                 delimiter=",", where_clause=None):
        self.uri_struct = uri_struct
        self.skip_lines = skip_lines
        self.field_names = field_names
        self.logger = logger
        self.delimiter = delimiter
        self.cleanup = []
        self.where_clause = where_clause

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
            "postgres": self.__make_sqla_source,
        }
        return dispatcher[uri_struct["driver"]](uri_struct)

    def __make_hdf5_source(self, uri_struct):
        """instantiate an hdf5 source"""
        accessor = ext_tables.PyTablesAccessor(uri_struct["database"], mode="r")
        full_path = uri_struct["entity"]
        where_clause = self.where_clause if self.where_clause else uri_struct["filter"]
        self.cleanup.append(accessor)
        return ext_tables.PyTablesSource(
            accessor,
            full_path,
            where_clause,
            field_names=self.field_names,
            logger=self.logger,
        )

    def __make_sqla_source(self, uri_struct):
        """instantiate sqlite source"""
        uri = ext_sql_alchemy.as_sqla_url(uri_struct)
        accessor = ext_sql_alchemy.SQLAlchemyAccessor(uri, echo=False)
        self.cleanup.append(accessor)
        return ext_sql_alchemy.SQLAlchemyTableSource(
            accessor,
            uri_struct["entity"],
            where_clause=self.where_clause if self.where_clause else uri_struct["filter"],
            field_names=self.field_names,
            logger=self.logger,
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
            logger=self.logger
        )
        return src

    def __make_file_source(self, uri_struct):
        """make a file based reader"""

        the_source = SOURCE_MAP[uri_struct["dialect"]]
        if uri_struct["dialect"] in ["xlsx", "xls"]:
            src = the_source(
                [uri_struct["database"]],
                field_names=self.field_names,
                logger=self.logger,
                skip_lines=self.skip_lines
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
                    logger=self.logger,
                    skip_lines=self.skip_lines
                )
                self.cleanup.append(src)
                return src
            else:
                the_reader = READER_MAP[uri_struct["compression"]]
                src = the_source(
                    [the_reader(uri_struct["database"])],
                    field_names=self.field_names,
                    delimiter=self.delimiter,
                    logger=self.logger,
                    skip_lines=self.skip_lines
                )
                self.cleanup.append(src)
                return src

        # All others
        else:
            if uri_struct["database"] == "stdio":
                return the_source(
                    [reader.StdinReader()],
                    field_names=self.field_names,
                    logger=self.logger,
                )
            else:
                the_reader = READER_MAP[uri_struct["compression"]]
                if (uri_struct["dialect"] in BINARY_DIALECTS):
                    return the_source(
                        [the_reader(uri_struct["database"], mode="rb")],
                        field_names=self.field_names,
                        logger=self.logger,
                    )
                else:
                    return the_source(
                        [the_reader(uri_struct["database"])],
                        field_names=self.field_names,
                        logger=self.logger,
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
