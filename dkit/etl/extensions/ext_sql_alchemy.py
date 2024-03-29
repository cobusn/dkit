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
# 28 Jan 2021 Cobus Nel       Modified code to work with Oracle SID's
# 26 Apr 2021 Cobus Nel       Added additional reflection code
# =========== =============== =================================================

import importlib
import logging
import re
from urllib.parse import urlencode
from datetime import datetime
from typing import Dict, List
import itertools
from .. import (source, schema, sink, model, DEFAULT_LOG_TRIGGER)
from ... import CHUNK_SIZE, messages
from ...data import iteration
from ...exceptions import DKitETLException
from ...utilities.cmd_helper import LazyLoad
from ...utilities import identifier
from ...data.containers import DictionaryEmulator
from ...parsers.uri_parser import NETWORK_DIALECTS, parse

jinja2 = LazyLoad("jinja2")
ora = LazyLoad("cx_Oracle")
sqlalchemy = LazyLoad("sqlalchemy")

logger = logging.getLogger(__name__)


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


VALID_DIALECTS = NETWORK_DIALECTS


SCHEMA_MAP = {
    "float": sqlalchemy.Float,
    "integer": sqlalchemy.Integer,
    "int8": sqlalchemy.SmallInteger,
    "int16": sqlalchemy.SmallInteger,
    "int32": sqlalchemy.Integer,
    "int64": sqlalchemy.BigInteger,
    "decimal": sqlalchemy.Numeric,
    # TinyInt only exist in specific implementations, e.g. mysql
    # NB: Unsigned is casted UP to avoid overflow..
    "uint8": sqlalchemy.SmallInteger,
    "uint16": sqlalchemy.Integer,
    "uint32": sqlalchemy.BigInteger,
    "uint64": sqlalchemy.BigInteger,
    "string":   sqlalchemy.String,
    "boolean":  sqlalchemy.Boolean,
    "date":     sqlalchemy.Date,
    "datetime": sqlalchemy.DateTime,
    "binary": sqlalchemy.LargeBinary,
}

# map between SQL types and closest Canonnical type
TYPE_MAP = {
    "BINARY": "binary",
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
    "DOUBLE_PRECISION": "float",
    "Date": "date",
    "DateTime": "datetime",
    "ENUM": "string",                   # MYSQL ENUM
    "FLOAT": "float",
    "Float": "float",
    "IMAGE": "binary",
    "INT": "integer",
    "INTEGER": "integer",
    "Integer": "integer",
    "JSON": "string",
    "JSONB": "string",
    "LONGBLOB": "binary",
    "LONGTEXT": "string",
    "LargeBinary": "binary",
    "MEDIUMBLOB": "binary",
    "MEDIUMINT": "int32",             # MYSQL Medium Integer
    "MEDIUMTEXT": "string",
    "MONEY": "decimal",
    "NCHAR": "decimal",
    "NTEXT": "string",
    "NullType": "object",             # Unknown object. e.g. sqlite counter table
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
    "TINYTEXT": "string",           # MYSQL specific
    "Time": "time",
    "Unicode": "string",
    "VARBINARY": "binary",
    "VARCHAR": "string",
}


class URL(object):

    """create SQlAlchemy URL from parameters"""
    def __init__(self, dialect, driver=None, username=None, password=None, host=None, port=None,
                 database=None, parameters=None, **kwargs):
        self.dialect = dialect
        self.drivername = driver
        self.username = username
        self.password = password
        self.host = host
        if port is not None:
            self.port = int(port)
        else:
            self.port = None
        self.database = database
        self.parameters = parameters

    @property
    def _user(self):
        """create user / password portion of url"""
        rv = ""
        if self.username is not None:
            rv = _rfc_1738_quote(self.username)
            if self.password is not None:
                rv += ":" + _rfc_1738_quote(self.password)
            rv += "@"
        return rv

    @property
    def _uri(self):
        rv = ""
        if "oracle" in self.drivername:
            # create Oracle DSN
            return ora.makedsn(
                self.host,
                self.port,
                service_name=self.database
            )
        else:
            if self.host is not None:
                if ":" in self.host:
                    rv += "[%s]" % self.host
                else:
                    rv += self.host
            if self.port is not None:
                rv += ":" + str(self.port)
            if self.database is not None:
                rv += "/"
                rv += self.database
        return rv

    @property
    def _options(self):
        if self.parameters is not None:
            return "?" + urlencode(self.parameters)
        else:
            return ""

    def __str__(self):
        return f"{self.dialect}://{self._user}{self._uri}{self._options}"


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
        logger.debug("connecting to database")
        self.engine = self.make_engine(url, echo)
        self.metadata = self.sqlalchemy.MetaData(bind=self.engine)
        self.__inspect = None

    def make_engine(self, url: str, echo: bool):

        if url.startswith("mssql+pyodbc"):
            # dealing with an azure mfa auth
            from sqlalchemy import event
            from azure import identity
            import struct

            st = parse(url)
            del st["username"]
            del st["password"]
            conns = as_sqla_url(st)
            breakpoint()
            engine = self.sqlalchemy.create_engine(
                url,
                echo=echo,
            )

            SQL_COPT_SS_ACCESS_TOKEN = 1256
            TOKEN_URL = "https://database.windows.net/"

            @event.listens_for(self.engine, "do_connect")
            def provide_token(dialect, conn_rec, cargs, cparams):
                # remove the "Trusted_Connection" parameter that SQLAlchemy adds
                cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "")
                azure_credentials = identity.DefaultAzureCredential()

                # create token credential
                raw_token = azure_credentials.get_token(TOKEN_URL).token.encode("utf-16-le")
                token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)

                # apply it to keyword arguments
                cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

        engine = self.sqlalchemy.create_engine(
            url,
            echo=echo,
        )
        return engine

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

    def execute(self, statement, multiple=True):
        """excecute driver level sql e.g for DDL"""
        with self.engine.connect() as conn:
            result_set = conn.exec_driver_sql(statement)
            try:
                for row in result_set:
                    yield dict(row._mapping.items())
            except self.sqlalchemy.exc.ResourceClosedError:
                logger.info("query did not return any rows")

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

    def iter_select(self, sql, log_trigger=DEFAULT_LOG_TRIGGER,
                    chunk_size=CHUNK_SIZE) -> "SQLAlchemySelectSource":
        """return iterator for select statement"""
        return SQLAlchemySelectSource(
            self,
            sql,
            log_trigger=log_trigger,
            chunk_size=chunk_size
        )


class SQLAlchemyReflector(object):
    """
    Create ETL model.Entity from SQL database

    Reflect table using SQLAlchemy inspector
    """
    def __init__(self, accessor: SQLAlchemyAccessor):
        self.accessor = accessor
        self.sql_alchemy = accessor.sqlalchemy
        self.logger = logging.getLogger(self.__class__.__name__)
        self.c_map = TYPE_MAP

    def get_table_names(self):
        """
        list of table names

        returns:
            list of table names
        """
        return self.accessor.inspect.get_table_names()

    def extract_profile(self, *tables):
        """extract profile for tables"""
        retval = {}
        for tbl_name in tables:
            _table = {}
            retval[tbl_name] = _table
            _table["schema"] = self.reflect_entity(tbl_name).as_dict()
            _table["relations"] = {
                k: v.as_dict()
                for k, v in self.reflect_relations(tbl_name).items()
            }

        return retval

    def reflect_relations(self, entity_name):
        """
        reflect foreign key relations for table: entity_name
        """
        def get_name(relation):
            name = relation["name"]
            if name:
                return name
            else:
                return identifier.obj_adler32(relation)

        def from_dict(entry):
            return model.Relation(
                constrained_entity=entity_name,
                constrained_columns=entry["constrained_columns"],
                referred_entity=entry["referred_table"],
                referred_columns=entry["referred_columns"],
            )

        relations = self.accessor.inspect.get_foreign_keys(entity_name)
        return {
            get_name(i): from_dict(i) for i in relations
        }

    def reflect_entity(self, entity_name):
        """
        reflect database entity to model.Entity

        args:
            entity_name: name of entity

        returns:
            model.Entity
        """
        self.logger.info(f"Reflecting table '{entity_name}'")
        columns = self.accessor.inspect.get_columns(entity_name)
        pk = self.accessor.inspect.get_pk_constraint(entity_name)
        try:
            indexes = self.accessor.inspect.get_indexes(entity_name)
        except AttributeError:
            # catch error with SQLAlchemy / postgresql
            logger.error(f"Error extracting indexes for {entity_name}")
            indexes = []
        retval = {}
        for ref_col in columns:
            _type = ref_col["type"]
            _name = ref_col["name"]
            ref_col["type"] = self.c_map[_type.__class__.__name__]
            if ref_col["type"] == "string":
                if hasattr(_type, "length") and _type.length:
                    ref_col["str_len"] = _type.length
            if ref_col["type"] == "decimal":
                if _type.precision:
                    ref_col["precision"] = _type.precision
                if _type.scale:
                    ref_col["scale"] = _type.scale
            if "primary_key" in ref_col:
                if ref_col["primary_key"] == 1:
                    ref_col["primary_key"] = True
                else:
                    del ref_col["primary_key"]
            del ref_col["name"]
            if "comment" in ref_col:
                del ref_col["comment"]
            del ref_col["nullable"]
            if "computed" in ref_col:
                ref_col["computed"] = True
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
        self.schema_map = SCHEMA_MAP

    def __get_dialect(self, dialect):
        if dialect not in VALID_DIALECTS:
            raise DKitETLException(
                messages.MSG_0020.format(dialect)
            )
        if "+" in dialect:
            d = dialect.split("+")[0]
        else:
            d = dialect
        dialects = importlib.import_module("sqlalchemy.dialects")
        return dialects.registry.load(d)

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
            if dialect == "awsathena":
                # athena require the location parameter
                t_instance = _Table(
                    _name, _metadata, *_model,
                    awsathena_location="${PATH}",
                    awsathena_file_format="PARQUET",
                    awsathena_compression="SNAPPY"
                )
            else:
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
            _dialect = self.__get_dialect(dialect)()
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
        try:
            result = conn.execute(selector)
            chunk = result.fetchmany(self.chunk_size)
            while len(chunk) > 0:
                yield from (dict(row._mapping.items()) for row in chunk)
                self.stats.increment(len(chunk))
                chunk = result.fetchmany(self.chunk_size)
        except self.sqlalchemy.exc.ResourceClosedError:
            logger.info("query did not return any rows")
        finally:
            logger.info("closing sql connection")
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
                 chunk_size=CHUNK_SIZE, limit=None):
        super().__init__(accessor, field_names=field_names, log_trigger=log_trigger,
                         chunk_size=chunk_size)
        self.table_name = table_name
        self.where_clause = where_clause or ""
        self.limit = limit

    def iter_some_fields(self, field_names):
        the_table = self.sqlalchemy.Table(
            self.table_name,
            self.accessor.metadata,
            autoload=True
        )
        where_clause = self.sqlalchemy.sql.text(self.where_clause)
        fields = [getattr(the_table.c, n) for n in field_names]
        s = self.sqlalchemy.select(fields, whereclause=where_clause)
        if self.limit:
            s = s.limit(self.limit)
        yield from self.iter_results(s)

    def iter_all_fields(self):
        the_table = self.sqlalchemy.Table(
            self.table_name,
            self.accessor.metadata,
            autoload=True
        )
        where_clause = self.sqlalchemy.sql.text(self.where_clause)
        s = self.sqlalchemy.select([the_table], whereclause=where_clause)
        if self.limit:
            s = s.limit(self.limit)
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


class SQLAlchemyTemplateSource(SQLAlchemyAbstractSource, DictionaryEmulator):
    """
    Create iterator from Jinja2 Template SQL statement

    Args:
        accessor: SQLAlchemyAccessor instance
        template:  SQL select statement template
        variables: dict containing variables
        log_trigger: trigger a log event every n rows
        ..

    This class will raise jinja2.exceptions.UndefinedError
    in the case of undefined variables

    Note that this class act as a Dict and the dictionary
    interface can be used to define variables
    """
    def __init__(self, accessor, template: str, variables: Dict = None,
                 log_trigger=DEFAULT_LOG_TRIGGER,
                 chunk_size=CHUNK_SIZE):
        SQLAlchemyAbstractSource.__init__(
            self, accessor,
            log_trigger=log_trigger, chunk_size=chunk_size
        )
        _vars = variables if variables else {}
        DictionaryEmulator.__init__(self, _vars)
        self.sqlo = importlib.import_module("sqlalchemy.sql")
        self.template = template

    def discover_parameters(self) -> List:
        """find variables in template"""
        env = jinja2.Environment()
        ast = env.parse(self.template)
        return list(jinja2.meta.find_undeclared_variables(ast))

    def get_rendered_sql(self):
        """get the rendered template"""
        tpl = jinja2.Template(
            self.template,
            undefined=jinja2.StrictUndefined
        )
        return tpl.render(self.store)

    def iter_all_fields(self):
        """
        raises
            - jinja2.exceptions.UndefinedError
        """
        _sql = self.sqlo.text(self.get_rendered_sql())
        yield from self.iter_results(_sql)


class SQLAlchemySink(sink.AbstractSink):
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
        for chunk in iteration.chunker(the_iterable, self.commit_rate):
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

    def get_sql_accessor(self, conn_name: str) -> SQLAlchemyAccessor:
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

    def get_sql_table_schema(
        self, conn_name: str, table_name: str, append=False
    ) -> model.Entity:
        """
        infer schema from database

        args:
            * conn_name: connection name
            * table_name: table name
            * append: append to model if True

        returns:
            schema

        """
        accessor = self.get_sql_accessor(conn_name)
        reflector = SQLAlchemyReflector(accessor)
        _entity = reflector.reflect_entity(table_name)
        if append:
            self.model.entities[table_name] = _entity
        return _entity

    def get_sql_table_relations(self, conn_name: str, table_name: str, append=False):
        """reflect table relations

        Parameters:
            * conn_name: connection
            * table_name: name of table
            * append: append to model

        """
        accessor = self.get_sql_accessor(conn_name)
        reflector = SQLAlchemyReflector(accessor)
        _relations = reflector.reflect_relations(table_name)
        if append:
            for name, relation in _relations.items():
                self.model.relations[name] = relation
        return _relations

    def run_template_query(self, connection: model.Connection, template, variables):
        """execute template query"""
        accessor = SQLAlchemyAccessor(as_sqla_url(connection.as_dict(True)))
        yield from SQLAlchemyTemplateSource(
            accessor,
            template,
            variables
        )
        accessor.close()

    def run_query(self, connection: model.Connection, query: str):
        """execute query and return results"""
        accessor = SQLAlchemyAccessor(as_sqla_url(connection.as_dict(True)))
        yield from SQLAlchemySelectSource(
            accessor,
            query,
        )
        accessor.close()

    def sample_from_db(self, conn_name, *tables, n=1000):
        """sample n records from each table in a database

        Arguments:
            - connection: connection name
            - n: number of records to sample
            - *tables: list of tables, all if not specified

        Returns:
            - Dictionary table names as key and records as value

        """
        accessor = self.get_sql_accessor(conn_name)
        if len(tables) == 0:
            _tables = self.get_sql_tables(conn_name)
        else:
            _tables = tables
        retval = {}
        for table_name in _tables:
            logger.info(f"sampling from '{table_name}'")
            rows = SQLAlchemyTableSource(accessor, table_name, chunk_size=n, limit=n)
            retval[table_name] = list(itertools.islice(rows, n))
        return retval
