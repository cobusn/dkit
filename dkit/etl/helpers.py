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
import logging
import re

from .extensions.ext_sql_alchemy import SQLServices, SQLAlchemyAccessor
from ..utilities.jinja2 import render_strict
from .model import Connection
import xxhash
import diskcache
import os
from textwrap import dedent
logger = logging.getLogger(__name__)


DEFAULT_CACHE_EXPIRATION = 86400  # on day


def is_select_statement(sql_string):
    """perform a basic test to see if a string is likely a SQL query

    Args:
        : The string to check.

    Returns:
        True if the string appears to be a SELECT statement, False otherwise.
    """
    pattern = r"^\s*SELECT\s+.+?\s+FROM\s+.+?(?:\s+WHERE\s+.+?)?\s*;?\s*$"
    match = re.match(pattern, sql_string, re.IGNORECASE | re.DOTALL)
    return bool(match)


class SQLETL:
    """
    SQL ETL utility

    If this class is Subclassed, the Docstring can be SQL and
    will be used if an SQL statement is not supplied

    Args:
        - services: SQLServices instance
        - connection: connection name
    """
    def __init__(self, services: SQLServices, connection: str):
        self.services = services
        self.connection = connection

    def _get_docstring_sql(self):
        """
        check if the docstring is SQL and return this as SQL
        Useful for quick work

        Raises Exception if the docstring does not match SQL
        """
        if is_select_statement(self.__doc__):
            return dedent(self.__doc__)
        else:
            raise ValueError("docstring does not appear to be valid SQL")

    def _extract(self, sql):
        logging.debug(sql)
        conn = self.services.model.get_connection(self.connection)
        return list(
            self.services.run_query(conn, sql)
        )

    def _render(self, sql: str, variables: dict):
        if variables:
            _sql = render_strict(sql, **variables)
        else:
            _sql = sql
        logger.debug(_sql)
        return _sql

    def render_sql(self, sql=None, params=None):
        """build sql from provided or docstring and parameters"""
        return self._render(
            sql or self._get_docstring_sql(),
            params
        )

    def extract(self, sql, params):
        """extract data"""
        return self._extract(self.render_sql(sql, params))

    def transform(self, data):
        return data

    def load(self, data):
        return list(data)

    def run(self, sql=None, params: dict = None):
        """run query

        args:
            - sql: sql string or template
            - params: dictionary of parameters

        """
        return self.load(
            self.transform(
                self.extract(sql, params)
            )
        )


class ConnectionSQLETL(SQLETL):
    """
    SQLETL that is instantiated from a connection and not model

    Useful if connections are stored in config file and not model
    """
    def __init__(self, connection: Connection):
        self.connection = connection

    def _extract(self, sql):
        logging.debug(sql)
        accessor = SQLAlchemyAccessor(self.connection.as_dict())
        yield from accessor.iter_select(sql)


class CachedSQLETL(SQLETL):
    """
    SQL ETL utility that cache results based on the SQL statment
    useful to reduce round trips to the database

    If this class is Subclassed, the Docstring can be SQL and
    will be used if an SQL statement is not supplied

    Args:
        - services: SQLServices instance
        - connection: connection name
        - cache_folder: name of folder used for cache
        - cache_name: name of cache
        - cache_expire: seconds until expiration
        - disable_cache: retrieve from cache if available else from query
    """
    def __init__(self, services: SQLServices, connection: str,
                 cache_folder: str = "cache", cache_name: str = None,
                 cache_expire: int = DEFAULT_CACHE_EXPIRATION,
                 disable_cache: bool = False):
        self._cache_folder = cache_folder
        self._cache_name = cache_name if cache_name else self.__class__.__name__
        self.expire = cache_expire
        self.cache = diskcache.Cache(self.cache_path)
        self.services = services
        self.connection = connection
        self.disable_cache = disable_cache

    @property
    def cache_path(self):
        return os.path.join(self._cache_folder, self._cache_name)

    def extract(self, sql, params) -> list[dict]:
        """extract data"""
        base_sql = sql or self._get_docstring_sql()
        rendered = self._render(base_sql, params)
        key = xxhash.xxh3_64_intdigest(rendered.encode('utf-8'))
        if key in self.cache and not self.disable_cache:
            logger.info(f"loading data from cache {self._cache_name}")
            return self.cache.get(key)
        else:
            data = list(self._extract(rendered))
            self.cache.set(key, data, expire=self.expire)
            return data
