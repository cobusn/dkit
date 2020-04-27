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
ETL extension for hdf5

=========== =============== =================================================
DATE        NAME            COMMENT
=========== =============== =================================================
May 2019    Cobus Nel       Added PyTablesServices
                            Improved read and write performance
Jul 2019    Cobus Nel       added list tables and reflect capability
=========== =============== =================================================
"""
from os import path
import datetime
from .. import (source, schema, sink, model, DEFAULT_LOG_TRIGGER)
from ... import exceptions
from ... import messages
from ...utilities.iter_helper import chunker
from ... import CHUNK_SIZE


MISSING_DATE_FLOAT = datetime.datetime(1970, 1, 2).timestamp()
MISSING_DATE_INT = datetime.date(1970, 1, 2).strftime("%s")


def convert_time64(x):
    try:
        return x.timestamp()
    except AttributeError:
        return MISSING_DATE_FLOAT


def convert_time32(x):
    try:
        return int(datetime(x.year, x.month, x.day).timestamp())
    except AttributeError:
        return MISSING_DATE_INT


def convert_string(x):
    try:
        return x.encode()
    except AttributeError:
        return str(x).encode()


def convert_int64(x):
    try:
        return int(x)
    except TypeError:
        return 0


def convert_float64(x):
    try:
        return float(x)
    except TypeError:
        return 0.0


def convert_bool(x):
    try:
        return bool(x)
    except TypeError:
        return False


def parse_fullpath(full_path: str):
    """
    parse full path to node_path, node_name tuple
    """
    if not full_path.startswith("/"):
        raise exceptions.CkitETLException(messages.MSG_0015)
    full_path = full_path.strip()
    l_path = path.split(full_path.strip())
    if len(l_path[0]) == 0:
        node_path = "/"
        node_name = full_path
    else:
        node_path = "/".join(l_path[:-1])
        node_name = l_path[-1]
    return (node_path, node_name)


class PyTablesAccessor(object):
    """
    accessor for pytable files (hdf5)
    """
    def __init__(self, file_name, mode="a", driver="H5FD_STDIO"):
        self.tables = __import__("tables")
        self.file_name = file_name
        self.driver = driver
        self.mode = mode
        self.h5_file = None
        self.h5_file = self.tables.open_file(self.file_name, self.mode, driver=self.driver)

    def create_table(self, full_path: str, schema, title: str = '',
                     complevel: int = 5, complib: str = 'blosc', createparents: bool = True):
        """
        create table in an hdf5 database

        Args:
            - full_path: path to node
            - schema: Cerberus schema
            - title: table description
            - complib compression library default to `blosc`
            - complevel int: compression level, default to 5
            - createparents: default to True
        """
        path, node_name = parse_fullpath(full_path)
        filters = self.tables.Filters(complevel=complevel, complib=complib)
        model = PyTablesModelFactory().create_model(schema)
        table = self.h5_file.create_table(
            path,
            node_name,
            model,
            title,
            filters,
            createparents=createparents
        )
        return table

    def create_index(self, full_path: str, column: str):
        """
        create index

        Args:
            full_path: path to node
            column: column to index
        """
        path, node_name = parse_fullpath(full_path)
        table = self.h5_file.get_node(path, node_name)
        col = table.colinstances[column]
        col.create_csindex()

    def get_table(self, full_path):
        """
        convenience function to return a table given a full path
        """
        node_path, node_name = parse_fullpath(full_path)
        return self.h5_file.get_node(node_path, node_name)

    def get_node_info(self, start="/"):
        """
        yields dicts containing
        - node name
        - node type
        """
        nodes = self.h5_file.walk_nodes(start)
        for node in nodes:
            retval = {
                "type": node.__class__.__name__,
                "name": node._v_name,
                "path": node._v_pathname,
            }
            if isinstance(node, self.tables.Table):
                retval["size"] = int(node.nrows)
            elif isinstance(node, self.tables.Group):
                retval["size"] = int(node._v_nchildren)
            else:
                retval["size"] = node.__class__.__name__
            yield retval

    def get_node_schema(self, full_path):
        return PyTablesReflector(self).reflect_table(full_path)

    def get_table_columns(self, full_path):
        """
        returns node.coldesrs
        """
        node = self.get_table(full_path)
        return node.coldescrs

    def close(self):
        if hasattr(self, "h5_file") and self.h5_file is not None:
            self.h5_file.close()

    def __del__(self):
        self.close()


class PyTablesSource(source.AbstractRowSource):
    """
    Iterator source to read rows from hdf5 using pytables

    Args:
        accessor: PyTables accessor
        full_path: full path to node
        where_clause: in core where clause
        field_names: list of field names to extract
        logger: logger instance
        log_template: log template
        log_trigger: trigger on multiples of this number
    """
    def __init__(self, accessor: PyTablesAccessor, full_path: str,
                 where_clause: str = None, field_names: list = None,
                 logger=None, log_template: str = None, log_trigger: int = DEFAULT_LOG_TRIGGER,
                 chunk_size: int = CHUNK_SIZE):
        super().__init__(logger=logger, log_template=log_template, log_trigger=log_trigger)
        self.accessor = accessor
        self.node_path, self.node_name = parse_fullpath(full_path)
        self.where_clause = where_clause
        self.field_names = field_names
        self.table = self.accessor.h5_file.get_node(self.node_path, self.node_name)
        self.chunk_size = chunk_size

    @property
    def nrows(self):
        """number of rows in table"""
        return self.table.nrows

    @staticmethod
    def get_mapped_fields(table):
        """
        create dictionary with conversion functions for fields
        that require mapping between python and hdf5 data types
        (datetime, date, string)

        Returns
            - dict of field_names: type for dat types
        """
        tmap = {
            "string": lambda x: x.decode('UTF-8'),
            "time32": lambda x: datetime.date.fromtimestamp(x),
            "time64": lambda x: datetime.datetime.fromtimestamp(x),
            "int64": lambda x: int(x),
            "float64": lambda x: float(x),
            "bool": lambda x: bool(x)
        }
        return {
            k: tmap[v.type]
            for k, v in table.coldescrs.items()
            # if v.type in tmap
        }

    def iter_all_fields(self):
        """
        yield dictionary of rows
        convert to python types
        """
        stats = self.stats.start()
        cmap = self.get_mapped_fields(self.table)
        cmap_items = [(i, k, v) for i, (k, v) in enumerate(cmap.items())]

        if self.where_clause:
            rows = self.table.where(self.where_clause)
            for row in rows:
                retval = {k: fn(row[i]) for i, k, fn in cmap_items}
                yield retval
                stats.increment()
        else:
            table = self.table
            i_start = 0
            i_stop = self.chunk_size
            chunk = table.read(i_start, i_stop)
            while len(chunk) > 0:
                for row in chunk:
                    retval = {k: fn(row[i]) for i, k, fn in cmap_items}
                    yield retval
                stats.increment(len(chunk))
                i_start += self.chunk_size
                i_stop += self.chunk_size
                chunk = table.read(i_start, i_stop)
        stats.stop()

    def iter_one_field(self, field_name):
        """
        yield dictionary of rows
        convert to python types
        """
        stats = self.stats.start()
        cmap = self.get_mapped_fields(self.table)
        typemap = cmap[field_name]

        if self.where_clause:
            rows = self.table.where(self.where_clause)
            yield from ({field_name: typemap(i)} for i in rows)
        else:
            i_start = 0
            i_stop = self.chunk_size
            chunk = self.table.read(i_start, i_stop, field=field_name)
            while len(chunk) > 0:
                yield from ({field_name: typemap(i)} for i in chunk)
                stats.increment(len(chunk))
                i_start += self.chunk_size
                i_stop += self.chunk_size
                chunk = self.table.read(i_start, i_stop, field=field_name)
        stats.stop()

    def iter_some_fields(self, field_names):
        """
        yield dictionary of rows
        convert to python types
        """
        stats = self.stats.start()
        cmap = self.get_mapped_fields(self.table)
        cmap_items = [(i, k, v) for i, (k, v) in enumerate(cmap.items()) if k in field_names]

        if self.where_clause:
            # Where clause specified
            rows = self.table.where(self.where_clause)
            for row in rows:
                retval = {k: fn(row[i]) for i, k, fn in cmap_items}
                yield retval
                stats.increment()
        else:
            # no where clause specified
            i_start = 0
            i_stop = self.chunk_size
            chunk = self.table.read(i_start, i_stop)
            while len(chunk) > 0:
                for row in chunk:
                    retval = {k: fn(row[i]) for i, k, fn in cmap_items}
                    yield retval
                stats.increment(len(chunk))
                i_start += self.chunk_size
                i_stop += self.chunk_size
                chunk = self.table.read(i_start, i_stop)

        stats.stop()


class PyTablesSink(sink.Sink):
    """
    serialize iteratable of dicts to pytables table

    Args:
    - accessor: accessor to HDF5 file
    - node_path: folder path to node
    - node: node to access
    - field_names: limit extract to these field names
    - logger: logger instance
    - commit_rate: commit at these intervals
    """
    def __init__(self, accessor: PyTablesAccessor, full_path: str, field_names: list = None,
                 logger=None, log_template: str = None, commit_rate: int = 1000):
        super().__init__(logger=logger, log_template=log_template)
        self.accessor = accessor
        self.field_names = field_names
        self.path, self.node = parse_fullpath(full_path)
        self.commit_rate = commit_rate
        self.table = accessor.get_table(full_path)

    @staticmethod
    def get_mapped_fields(table):
        """
        create dictionary with conversion functions for fields
        that require mapping between python and hdf5 data types
        (datetime, date, string)

        Returns
            - dict of field_names: type for dat types
        """

        tmap = {
            "string": convert_string,
            "time64": convert_time64,
            "time32": convert_time32,
            "float64":  convert_float64,
            "int64": convert_int64,
            "bool": convert_bool,
        }
        return {
            k: tmap[v.type]
            for k, v in table.coldescrs.items()
        }

    def add_row(self, table, row):
        """
        add one row
        """
        # datetime
        h5row = 0
        for key in self.map_datetime:
            try:
                h5row[key] = row[key].timestamp()
            except (OSError, AttributeError) as E:
                # Avoid errors with timestamp 0
                h5row[key] = self.err_timestamp
                self.logger.error("Error {} on key {} for row {}".format(E, key, row))

        # string
        for key in self.map_str:
            try:
                h5row[key] = bytes(str(row[key]), "ascii", errors="ignore")
            except Exception as E:
                self.logger.error("Error {} on key {} for row {}".format(E, key, row))
                h5row[key] = b''
        h5row.append()

    def process(self, the_iterable):
        """
        process the iterable.
        """
        cmap = self.get_mapped_fields(self.table)

        def convert(row):
            for k, v in cmap.items():
                row[k] = v(row[k])
            return tuple(row.values())

        chunk_size = 100
        stats = self.stats.start()
        colnames = self.table.colnames
        for chunk in chunker(the_iterable, size=chunk_size):
            chunk = [convert({k: row[k] for k in colnames}) for row in chunk]
            self.table.append(chunk)
            stats.increment(len(chunk))
            self.table.flush()
        stats.stop()

        return self


class PyTablesReflector(object):
    """
    Create ETL model.Entity schema HDF5 table
    """
    def __init__(self, accessor: PyTablesAccessor):
        self.accessor = accessor
        self.c_map = {
            "bool": "boolean",
            "float12": "float",
            "float16": "float",
            "float32": "float",
            "float64": "float",
            "float96": "float",
            "int8": "small_integer",
            "int16": "small_integer",
            "int32": "integer",
            "int64": "integer",
            "string": "string",
            "time32": "date",
            "time64": "date",
            "uint16": "dattime",
            "uint32": "integer",
            "uint64": "integer",
            "uint8": "integer",
        }

    def reflect_table(self, full_path: str):
        """
        reflect to model.Entity

        args:
            - full_path: full path to entity (e.g "/sales/jan")
        returns:
            model.Entity
        """
        retval = {}
        node = self.accessor.get_table(full_path)
        # print(node.colindexes)
        for name, column in node.coldescrs.items():
            # print(dir(column), column)
            this = {}
            this["type"] = self.c_map[column.type]
            if this["type"] == "string":
                this["str_len"] = column.size
            if name in node.colindexes:
                this["index"] = True
            retval[name] = this
        return model.Entity.from_cerberus(retval)


class PyTablesModelFactory(schema.ModelFactory):
    """
    Create pytables model description from cerberus schema.
    """

    def __init__(self, default_str_len=255):
        self.tables = __import__("tables")
        self.schema_map = {
                "float":  self.tables.Float64Col,
                "integer": self.tables.Int64Col,
                "string": self.tables.StringCol,
                "boolean": self.tables.BoolCol,
                "date": self.tables.Time64Col,
                "datetime": self.tables.Time64Col,
                "decimal": self.tables.Float64Col,
        }
        super(PyTablesModelFactory, self).__init__(default_str_len)

    def create_model(self, validator):
        """
        create model from cerberus validator instance
        """
        schema = validator.schema
        mapping = {}
        for key, rules in schema.items():
            the_type = rules["type"]
            if the_type == "string":
                try:
                    strlen = schema[key]["str_len"]
                except Exception:
                    strlen = self.default_str_len
                mapping[key] = self.schema_map[the_type](strlen)
            else:
                mapping[key] = self.schema_map[the_type]()
        return mapping


class PyTablesServices(model.ETLServices):
    """
    helpers for working with hdf5 databases
    """
    def create_table(self, endpoint_name):
        """
        create hdf5 table

        Args:
            endpoint_name: model endpoint reference
        """
        i_endpoint = self.model.endpoints[endpoint_name]
        i_entity = self.model.entities[i_endpoint.entity]
        accessor = self.get_accessor(i_endpoint.connection)
        accessor.create_table(
            full_path=i_endpoint.table_name,
            schema=i_entity.as_entity_validator(),
            title=endpoint_name
        )

    def get_accessor(self, conn_name: str):
        """
        return hdf5 extension accessor

        accessor is cached for re-use

        args:
            * conn_name: connection name
        returns:
            accessor
        """
        connection = self.model.connections[conn_name]
        return PyTablesAccessor(
            connection.database,
            mode="a"
        )

    def do_h5_reflect(self, file_name, full_path):
        """reflect entity in a HDF5 database
        arguments:
            - file_name: name of hdf5 database file
            - full_path: path to entity
        returns
            - Entity instance
        """
        if not path.exists(file_name):
            raise exceptions.CkitETLException(f"file {file_name} not found")
        accessor = PyTablesAccessor(file_name)
        entity = accessor.get_node_schema(full_path)
        return entity

    def get_table_names(self, database_name: str):
        """
        list of hdf5 table names

        args:
            * conn_name: name of connection in model

        returns:
            list of table names
        """
        if not path.exists(database_name):
            raise exceptions.CkitETLException(f"file {database_name} not found")
        accessor = PyTablesAccessor(database_name, "r")
        return accessor.get_node_info()
