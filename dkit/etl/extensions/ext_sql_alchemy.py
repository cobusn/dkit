#
# Copyright (C) 2017 Cobus Nel
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
# =========== =============== =================================================
# 27 Nov 2019 Cobus Nel       Added facility for options in URL
# =========== =============== =================================================
import importlib
import logging
from .. import (source, schema, sink, model, DEFAULT_LOG_TRIGGER)
from ...utilities import iter_helper
from ... import CHUNK_SIZE
from ... import messages
from ...exceptions import CkitETLException
from datetime import datetime

from typing import Dict
import re


logger = logging.getLogger(__name__)


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


VALID_DIALECTS = [
    "firebird", "mssql", "mysql", "oracle", "postgresql", "sqlite", "sybase",
    "impala"
]


class URL(object):

    def __init__(self, driver, username=None, password=None, host=None, port=None,
                 database=None, options=None, **kwargs):
        self.drivername = driver
        self.username = username
        self.password = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database = database
        self.options = options

    def __str__(self):
        s = self.drivername + "://"
        if self.username is not None:
            s += _rfc_1738_quote(self.username)
            if self.password is not None:
                s += ":" + _rfc_1738_quote(self.password)
            s += "@"
        if self.host is not None:
            if ":" in self.host:
                s += "[%s]" % self.host
            else:
                s += self.host
        if self.port is not None:
            s += ":" + str(self.port)
        if self.database is not None:
            if not(("oracle" in self.drivername) and (self.host is None)):
                s += "/"
            s += self.database
        if self.options is not None:
            s += "?" + self.options
        return s


def as_sqla_url(uri_map: Dict[str, str]):
    """
    convert to uri struct to SqlAlchemy URL

    for use with SqlAlchemy
    """
    return str(URL(**uri_map))


class SQLAlchemyAccessor(object):
    """
    Accessor to SQLAlchemy supported database.

    Encapsulates SQLAlchemy engine and metadata

    Args:
        url:    SQLAlchemy URL. Refer to
                http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
        echo:   Echo SQL statements (Default is False)
    """
    def __init__(self, url: str, echo: bool = False, ):
        self.sqlalchemy = importlib.import_module("sqlalchemy")
        self.url = url
        logger.debug("Accessing database with url: {self.url}")
        self.engine = self.sqlalchemy.create_engine(
            url,
            echo=echo,
            # poolclass=self.sqlalchemy.pool.NullPool
        )
        self.metadata = self.sqlalchemy.MetaData(bind=self.engine)
        self.__inspect = None

    def __del__(self):
        self.close()

    def close(self):
        """Will log error if any occur"""
        try:
            if self.engine:
                self.engine.dispose()
        except Exception as E:
            logger.error(E)
        finally:
            self.engine = None
            self.__inspect = None
            self.metadata = None

    def create_table(self, table_name, validator_schema):
        """
        Create database table using SQLAlchemy model provided

        Args:
            table_name: table name
            model:  SQLAlchemy model

        Returns:
            None
        """
        model = SQLAlchemyModelFactory().create_model(validator_schema)
        the_table = self.sqlalchemy.Table(table_name, self.metadata, *model)
        self.metadata.create_all(self.engine, [the_table])

    @property
    def inspect(self):
        """
        return instantiated sqlalchemy.inspect object.

        The following commands are of interest:

            * get_table_names()
            * get_columns(<table_name>)
            * get_foreign_keys(<table_name>)
            * get_pk_constraint(<table_name>)
            * get_table_options(<table_name>)
            * get_check_constraints(<table_name>)
            * get_unique_constraints(<table_name>)
            * get_view_names()
            * get_view_definition(<view_name>)
        """
        if self.__inspect is None:
            self.__inspect = self.sqlalchemy.inspect(self.engine)
        return self.__inspect

    @classmethod
    def from_connection(cls, connection_instance, echo=False):
        """
        instantiate from model.connection instance
        """
        return cls(
            as_sqla_url(connection_instance.as_dict()),
            echo=echo
        )


class SQLAlchemyReflector(object):
    """
    Create ETL model.Entity from SQL database

    Reflect table using SQLAlchemy inspector
    """
    def __init__(self, accessor: SQLAlchemyAccessor):
        self.accessor = accessor
        self.sql_alchemy = accessor.sqlalchemy

        self.c_map = {
            "BIGINT": "int64",
            "BIT": "binary",
            "BLOB": "binary",
            "BOOLEAN": "boolean",
            "BigInteger": "int64",
            "Boolean": "boolean",
            "CHAR": "binary",
            "DATE": "date",
            "DATETIME": "datetime",
            "DATETIME2": "datetime",
            "DECIMAL": "decimal",
            "DOUBLE": "float",
            "Date": "date",
            "DateTime": "datetime",
            "ENUM": "string",                   # MYSQL ENUM
            "FLOAT": "float",
            "Float": "float",
            "IMAGE": "binary",
            "INT": "integer",
            "INTEGER": "integer",
            "Integer": "integer",
            "LONGBLOB": "binary",
            "LONGTEXT": "string",
            "LargeBinary": "binary",
            "MEDIUMBLOB": "binary",
            "MEDIUMINT": "int32",             # MYSQL Medium Integer
            "MEDIUMTEXT": "string",
            "MONEY": "decimal",
            "NCHAR": "decimal",
            "NTEXT": "string",
            "NUMERIC": "decimal",
            "NVARCHAR": "string",
            "Numeric": "decimal",
            "REAL": "float",
            "SMALLINT": "int16",
            "SMALLMONEY": "decimal",
            "SmallInteger": "int16",
            "STRING": "string",
            "String": "string",
            "TEXT": "string",
            "TIMESTAMP":  "datetime",
            "TINYINT": "int8",              # MYSQL specific
            "Time": "time",
            "Unicode": "string",
            "VARBINARY": "binary",
            "VARCHAR": "string",
        }

    def reflect_relations(self, entity_name):
        """
        reflect foreign key relations for table: entity_name
        """
        def from_dict(entry):
            return model.Relation(
                constrained_entity=entity_name,
                constrained_columns=entry["constrained_columns"],
                referred_entity=entry["referred_table"],
                referred_columns=entry["referred_columns"],
            )

        relations = self.accessor.inspect.get_foreign_keys(entity_name)
        return {
            i["name"]: from_dict(i) for i in relations
        }

    def reflect_entity(self, entity_name):
        """
        reflect database entity to model.Entity

        args:
            entity_name: name of entity

        returns:
            model.Entity
        """
        columns = self.accessor.inspect.get_columns(entity_name)
        pk = self.accessor.inspect.get_pk_constraint(entity_name)
        indexes = self.accessor.inspect.get_indexes(entity_name)
        retval = {}
        for ref_col in columns:
            _type = ref_col["type"]
            _name = ref_col["name"]
            ref_col["type"] = self.c_map[_type.__class__.__name__]
            if ref_col["type"] == "string":
                ref_col["str_len"] = _type.length
            if "primary_key" in ref_col:
                if ref_col["primary_key"] == 1:
                    ref_col["primary_key"] = True
                else:
                    del ref_col["primary_key"]
            del ref_col["name"]
            if "comment" in ref_col:
                del ref_col["comment"]
            del ref_col["nullable"]
            if "autoincrement" in ref_col:
                del ref_col["autoincrement"]
            if "default" in ref_col:
                del ref_col["default"]
            self.__process_primary_key(_name, ref_col, pk)
            self.__process_indexes(_name, ref_col, indexes)
            retval[_name] = ref_col
        return model.Entity.from_cerberus(retval)

    def __process_primary_key(self, field_name, ref_col, pk):
        """Identify primary key fields"""
        if 'constrained_columns' in pk:
            if field_name in pk['constrained_columns']:
                ref_col["primary_key"] = True

    def __process_indexes(self, field_name, ref_col, indexes):
        """Identify indexed fields"""
        idx = [i for i in indexes if field_name in i["column_names"]]
        if len(idx) > 0:
            ref_col["index"] = True
        if any([i["unique"] for i in idx]):
            ref_col["unique"] = True


class SQLAlchemyModelFactory(schema.ModelFactory):
    """
    Create SQLAlchemy model description from cerberus schema.

    args:
        default_str_len: default length for string
    """
    def __init__(self, default_str_len=255):
        super().__init__(default_str_len)
        self.sqlalchemy = importlib.import_module("sqlalchemy")
        self.schema_map = {
                "float": self.sqlalchemy.Float,
                "integer": self.sqlalchemy.Integer,
                "int8": self.sqlalchemy.SmallInteger,
                "int16": self.sqlalchemy.SmallInteger,
                "int32": self.sqlalchemy.Integer,
                "int64": self.sqlalchemy.BigInteger,
                "decimal": self.sqlalchemy.Numeric,
                "string":   self.sqlalchemy.String,
                "boolean":  self.sqlalchemy.Boolean,
                "date":     self.sqlalchemy.Date,
                "datetime": self.sqlalchemy.DateTime,
                "binary": self.sqlalchemy.Binary,
        }

    def __get_dialect(self, dialect):
        if dialect not in VALID_DIALECTS:
            raise CkitETLException(
                messages.MSG_0020.format(dialect)
            )
        _module_name = f"sqlalchemy.dialects.{dialect}"
        return importlib.import_module(_module_name)

    def create_sql_select(self, dialect: str,
                          **entities: Dict[str, model.Entity]) -> str:
        """
        create generic SQL select statement

        args:
            - dialect: database dialect e.g. mysql (as per SQLAlchemy)
            - entities: dict of entity names with mapped fields
        """
        _Table = self.sqlalchemy.Table
        _metadata, _dialect = self._create_metadata(dialect)
        _select = getattr(self.sqlalchemy.sql, "select")
        retval = f"\n-- Created on {datetime.now().strftime('%Y-%m-%d')}"
        for i, (_name, type_map) in enumerate(entities.items()):
            retval += f"\n\n--\n-- {_name}\n--\n"
            _model = self.create_model(type_map.schema)
            _statement = _select([_Table(_name, _metadata, *_model)])
            retval += str(_statement.compile(dialect=_dialect)).strip() + ";"
        return retval + "\n"

    def create_sql_schema(self, dialect: str,
                          **entities: Dict[str, model.Entity]) -> str:
        """
        create SQL Create statements from map of Entities

        args:
            - dialect: SQLAlchemy dialect name (e.g. mysql)
            - entities: Map of entities
        """
        _Table = self.sqlalchemy.Table
        _metadata, _dialect = self._create_metadata(dialect)
        _CreateTable = getattr(self.sqlalchemy.schema, "CreateTable")
        _CreateIndex = getattr(self.sqlalchemy.schema, "CreateIndex")
        retval = f"\n-- Created on {datetime.now().strftime('%Y-%m-%d')}"
        for i, (_name, type_map) in enumerate(entities.items()):
            retval += f"\n\n--\n-- {_name}\n--\n"
            _model = self.create_model(type_map.schema)
            t_instance = _Table(_name, _metadata, *_model)
            _statement = _CreateTable(t_instance)
            retval += str(_statement.compile(dialect=_dialect)).strip() + ";"
            # create indexes
            for index_ in t_instance.indexes:
                _statement = _CreateIndex(index_)
                retval += "\n" + str(_statement.compile(dialect=_dialect)).strip() + ";"

        return retval + "\n"

    def _create_metadata(self, dialect):
        _metadata = self.sqlalchemy.MetaData()
        if dialect is not None:
            _dialect = self.__get_dialect(dialect).dialect()
        else:
            _dialect = None
        return _metadata, _dialect

    def create_model(self, validator):
        """
        create model from schema instance

        Args:
            validator: schema validator
        """
        schema = validator.schema
        mapping = []
        for key, rules in schema.items():
            the_type = rules["type"]
            primary_key = True if "primary_key" in rules else False
            indexed = True if "index" in rules else False
            if the_type == "string":
                try:
                    strlen = schema[key]["str_len"]
                except Exception:
                    strlen = self.default_str_len
                col_type = self.schema_map[the_type](strlen)
            else:
                col_type = self.schema_map[the_type]()
            mapping.append(
                self.sqlalchemy.Column(key, col_type, primary_key=primary_key, index=indexed)
            )
        return mapping


class SQLAlchemyAbstractSource(source.AbstractRowSource):

    def __init__(self, accessor, field_names=None, log_trigger=DEFAULT_LOG_TRIGGER,
                 chunk_size=CHUNK_SIZE):
        super().__init__(field_names=field_names, log_trigger=log_trigger)
        self.sqlalchemy = importlib.import_module("sqlalchemy")
        self.accessor = accessor
        self.chunk_size = chunk_size

    def iter_results(self, selector):
        self.stats.start()
        conn = self.accessor.engine.connect().\
            execution_options(stream_results=True)
        result = conn.execute(selector)
        chunk = result.fetchmany(self.chunk_size)
        while len(chunk) > 0:
            yield from (dict(row.items()) for row in chunk)
            self.stats.increment(len(chunk))
            chunk = result.fetchmany(self.chunk_size)
        conn.close()
        self.stats.stop()


class SQLAlchemyTableSource(SQLAlchemyAbstractSource):
    """
    create iterator from database table.

    Args:
        accessor: SQLAlchemyAccessor instance
        table_name: name of table in database
        where_clause: SQL Where clause
        field_names: return only these fields
        log_trigger: trigger a log event every n rows
    """
    def __init__(self, accessor, table_name, where_clause=None, field_names=None,
                 log_trigger=DEFAULT_LOG_TRIGGER,
                 chunk_size=CHUNK_SIZE):
        super().__init__(accessor, field_names=field_names, log_trigger=log_trigger,
                         chunk_size=chunk_size)
        self.table_name = table_name
        self.where_clause = where_clause or ""

    def iter_some_fields(self, field_names):
        the_table = self.sqlalchemy.Table(
            self.table_name,
            self.accessor.metadata,
            autoload=True
        )
        where_clause = self.sqlalchemy.sql.text(self.where_clause)
        fields = [getattr(the_table.c, n) for n in field_names]
        s = self.sqlalchemy.select(fields, whereclause=where_clause)
        yield from self.iter_results(s)

    def iter_all_fields(self):
        the_table = self.sqlalchemy.Table(
            self.table_name,
            self.accessor.metadata,
            autoload=True
        )
        where_clause = self.sqlalchemy.sql.text(self.where_clause)
        s = self.sqlalchemy.select([the_table], whereclause=where_clause)
        yield from self.iter_results(s)


class SQLAlchemySelectSource(SQLAlchemyAbstractSource):
    """
    Create iterator from select statement.

    Args:
        accessor: SQLAlchemyAccessor instance
        select_stmt:  SQL select Statement
        log_trigger: trigger a log event every n rows
    """
    def __init__(self, accessor, select_stmt, log_trigger=DEFAULT_LOG_TRIGGER,
                 chunk_size=CHUNK_SIZE):
        super().__init__(accessor, log_trigger=log_trigger, chunk_size=chunk_size)
        self.sqlo = importlib.import_module("sqlalchemy.sql")
        self.select_stmt = select_stmt

    def iter_all_fields(self):
        stmt = self.sqlo.text(self.select_stmt)
        yield from self.iter_results(stmt)


class SQLAlchemySink(sink.Sink):
    """
    Insert records into database using SQLAlchemy

    Args:
        accessor: SQlAlchemyAccessor instance
        table_name: datbase table name
        commit_rate: database commit occur every n times
    """
    def __init__(self, accessor, table_name, chunk_size=CHUNK_SIZE):
        super().__init__()
        self.sqlalchemy = importlib.import_module("sqlalchemy")
        self.accessor = accessor
        self.table_name = table_name
        self.commit_rate = chunk_size

    def process(self, the_iterable):
        """
        Insert into database
        """
        the_table = self.sqlalchemy.Table(self.table_name, self.accessor.metadata, autoload=True)
        conn = self.accessor.engine.connect()

        stats = self.stats.start()
        for chunk in iter_helper.chunker(the_iterable, self.commit_rate):
            ins_chunk = list(chunk)
            conn.execute(
                the_table.insert(),
                ins_chunk
            )
            stats.increment(len(ins_chunk))
        self.stats.stop()
        conn.close()
        return self


class SQLServices(model.ETLServices):
    """
    Shared utilitiies that facilitate interfacing with SQL databases
    via SQLAlchemy and ext_sql_alchemy
    """
    def __init__(self, model_uri, config_uri):
        super().__init__(model_uri, config_uri)
        self.__accessor = {}

    def create_sql_table(self, endpoint_name):
        """
        create sql datbase table using sql alchemy

        Args:
            endpoint_name: model endpoint reference
        """
        i_endpoint = self.model.endpoints[endpoint_name]
        i_entity = self.model.entities[i_endpoint.entity]

        i_url = as_sqla_url(
            self.model.get_connection(i_endpoint.connection).as_dict(include_none=True)
        )

        # create the table
        accessor = SQLAlchemyAccessor(i_url, echo=True)
        accessor.create_table(
            i_endpoint.table_name,
            i_entity.as_entity_validator()
        )

    def get_sql_accessor(self, conn_name: str):
        """
        return sqlalchemy extension accessor

        accessor is cached for re-use

        args:
            * conn_name: connection name
        returns:
            accessor
        """
        if conn_name not in self.__accessor:
            conn_map = self.model.get_connection(conn_name)
            self.__accessor[conn_name] = SQLAlchemyAccessor(
                as_sqla_url(conn_map.as_dict())
            )
        return self.__accessor[conn_name]

    def get_sql_tables(self, conn_name: str):
        """
        list of  sql table names

        args:
            * conn_name: name of connection in model

        returns:
            list of table names
        """
        accessor = self.get_sql_accessor(conn_name)
        return accessor.inspect.get_table_names()

    def get_sql_table_schema(self, conn_name: str, table_name: str, append=False):
        accessor = self.get_sql_accessor(conn_name)
        reflector = SQLAlchemyReflector(accessor)
        _entity = reflector.reflect_entity(table_name)
        if append:
            self.model.entities[table_name] = _entity
        return _entity

    def get_sql_table_relations(self, conn_name: str, table_name: str, append=False):
        accessor = self.get_sql_accessor(conn_name)
        reflector = SQLAlchemyReflector(accessor)
        _relations = reflector.reflect_relations(table_name)
        if append:
            for name, relation in _relations.items():
                if name is None:
                    name = "{}_{}".format(
                        relation.constrained_entity.lower(),
                        relation.referred_entity.lower()
                    )
                self.model.relations[name] = relation
        return _relations

    def run_query(self, connection: model.Connection, query: str):
        """execute query"""
        accessor = SQLAlchemyAccessor(as_sqla_url(connection.as_dict(True)))
        yield from SQLAlchemySelectSource(
            accessor,
            query,
        )
        accessor.close()
