# Copyright (c) 2024 Cobus Nel
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
ETL helper classes for common tasks:

Extract:

    - extracting SQL
    - extracting SQL using templates

Load:
    - writing to S3
"""
import json
import logging
from abc import ABC, abstractmethod

from jinja2 import Template
from pyarrow.fs import FileSystem

from ..data.iteration import chunker
from .extensions.ext_arrow import build_table, make_arrow_schema, write_parquet_dataset
from .extensions.ext_athena import SchemaGenerator
from .extensions.ext_sql_alchemy import SQLServices
from .model import Entity
import xxhash
import diskcache
import os
logger = logging.getLogger(__name__)


class AbstractExtractor(ABC):

    @abstractmethod
    def __call__(self):
        pass


class CachedSQLETL:
    """
    SQL ETL utility that cache results based on the SQL statment
    useful to reduce round trips to the database
    """
    def __init__(self, services: SQLServices, connection: str,
                 cache_folder="cache", cache_name=None, cache_expire=None):
        self._cache_folder = cache_folder
        self._cache_name = cache_name if cache_name else self.__class__.__name__
        self.expire = cache_expire
        self.cache = diskcache.Cache(self.cache_path)
        self.services = services
        self.connection = connection

    @property
    def cache_path(self):
        return os.path.join(self._cache_folder, self._cache_name)

    def _extract(self, sql):
        conn = self.services.model.get_connection(self.connection)
        return list(
            self.services.run_query(
                conn, sql)
        )

    def extract(self, sql):
        """extract data"""
        key = xxhash.xxh3_64_intdigest(sql)
        if key in self.cache:
            return self.cache.get(key)
        else:
            data = self._extract(sql)
            self.cache.set(key, data, expire=self.expire)

    def transform(self, data):
        return data

    def load(self, data):
        return data

    def run(self, sql):
        return self.load(
            self.transform(
                self.extract(sql)
            )
        )


class SQLExtractor(AbstractExtractor):
    """SQL Extraction utility class"""

    def __init__(self, sql_services: SQLServices, conn: str):
        self.services = sql_services
        self.conn = conn

    def extract_schema(self, table_name):
        """reflect table schema from database"""
        schema = self.services.get_sql_table_schema(
            conn_name=self.conn,
            table_name=table_name
        )
        logger.debug(json.dumps(schema.as_dict(), indent=2))
        return schema

    def __call__(self, sql):
        """run query for interval between start and stop"""
        return self.services.run_query(
            self.services.model.get_connection(self.conn),
            sql
        )


class TemplateSQLExtractor(AbstractExtractor):
    """SQL Template query extractor

    Arguments:
        - sql_servcies: SQLServcies instance
        - conn: connection name
        - entity: entity name
        - query_name: query name (as stored in model)
        - query_str: SQL string

    query_name and query_str is mutually exclusive one must be
    defined.
    """

    def __init__(self, sql_services: SQLServices, conn: str, entity: str,
                 query_name: str = None, query_sql: str = None):
        self.services = sql_services
        self.conn = conn
        self.entity = entity
        self.query_name = query_name
        self.query_sql = query_sql
        if all([query_name, query_sql]) or (not any([query_name, query_sql])):
            raise ValueError("only one of `query_name' or 'query_sql' must be defined")

    def make_sql(self, **parameters):
        """create sql and render template"""
        if self.query_sql is not None:
            sql = Template(self.query_sql).render(**parameters)
        elif self.query_name is not None:
            query = self.services.model.queries[self.query_name]
            sql = query.template.render(**parameters)
        else:
            raise ValueError("only one of `query_name' or 'query_sql' must be defined")
        return sql

    def schema(self, table_name):
        """return entity schema"""
        return self.services.model.entities[self.entity]

    def __call__(self, **parameters):
        """run query for interval between start and stop"""
        return self.services.run_query(
            self.services.model.get_connection(self.conn),
            self.make_sql(**parameters)
        )


class AbstractLoader(ABC):

    @abstractmethod
    def __call__(self):
        pass


class ParquetLoader(AbstractLoader):
    """
    ETL data from SQL Database to Athena

    args:
        - schema: Entity instance
        - fs: FileSystem (e.g. S3FileSystem instance)
        - partition_columns: lis of partition columns or None
        - compression: e.g. snappy
        - chunk_size: max size of each parquet file
        - s3_path: destination path
        - existing_data_behaviour="delete_matching":
            ‘overwrite_or_ignore’ | ‘error’ | ‘delete_matching’
    """
    def __init__(self,
                 schema: Entity,
                 fs: FileSystem,
                 path: str,
                 partition_columns=None,
                 compression="snappy",
                 chunk_size=1_000_000,
                 existing_data_behaviour="delete_matching",
                 **kwargs):
        super().__init__(**kwargs)
        self.schema = schema
        self.path = path
        self.fs = fs
        self.schema = schema
        self.compression = compression
        self.chunk_size = chunk_size
        self.partition_columns = partition_columns if partition_columns else []
        self.existing_data_behaviour = existing_data_behaviour

    def make_ddl(self, table_name):
        """
        Generate DDL for Athena table
        """
        gen = SchemaGenerator(
            table_name=table_name,
            entity=self.schema,
            partition_by=self.partition_columns,
            kind="parquet",
            location=f"s3://{self.path}"
        )
        return gen.get_create_sql()

    def __call__(self, data):
        arrow_schema = make_arrow_schema(self.schema)
        logger.debug(str(arrow_schema))
        for chunk in chunker(data, self.chunk_size):
            table = build_table(chunk, schema=arrow_schema)
            if len(table) > 0:
                # dont write an empty table
                write_parquet_dataset(
                    table=table,
                    path=self.path,
                    partition_cols=self.partition_columns,
                    fs=self.fs,
                    compression=self.compression,
                    existing_data_behaviour=self.existing_data_behaviour
                )
