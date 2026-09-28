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
import sys; sys.path.insert(0, "..")  # noqa
import unittest
from unittest.mock import Mock
import yaml
from datetime import datetime
from dkit.etl.extensions import ext_sql_alchemy
from dkit.parsers.uri_parser import parse
from dkit.etl import (schema, transform)
from dkit.exceptions import DKitETLException
import jinja2

SCHEMA = """
id: {str_len: 11, type: string, primary_key: True}
birthday: {type: datetime}
company: {str_len: 32, type: string}
ip: {str_len: 15, type: string}
name: {str_len: 22, type: string}
score: {type: float}
year: {type: integer}
"""

# Inline sample rows — avoids dependency on missing input_files/sample.jsonl
SAMPLE_DATA = [
    {
        "id": str(i).zfill(11),
        "birthday": datetime(1990, 1, 1),
        "company": "ACME",
        "ip": "192.168.1.1",
        "name": "Alice",
        "score": float(i),
        "year": 2020 + i,
    }
    for i in range(10)
]
SAMPLE_COUNT = len(SAMPLE_DATA)

NORTHWIND = "sqlite:///data/Northwind_small.sqlite"
NORTHWIND_TABLE_NAMES = list(sorted([
    'Category', 'CustomerCustomerDemo', 'CustomerDemographic', 'Customer',
    'EmployeeTerritory', 'Employee', 'OrderDetail', 'Order', 'Product',
    'Region', 'Shipper', 'Supplier', 'Territory'
]))


class TestSQLAlchemyURL(unittest.TestCase):
    """Test SQLAlchemy URL construction without connecting to a database."""

    def test_oracledb_url_uses_service_name(self):
        """Oracle database names are emitted as service-name parameters."""
        connection = {
            "dialect": "oracle+oracledb",
            "username": "user",
            "password": "pass",
            "host": "db.example.com",
            "port": "1521",
            "database": "PROD",
            "parameters": {"events": "true"},
        }

        self.assertEqual(
            ext_sql_alchemy.as_sqla_url(connection),
            "oracle+oracledb://user:pass@db.example.com:1521?"
            "events=true&service_name=PROD",
        )

    def test_oracledb_thick_mode_is_engine_option(self):
        """Thick mode is not passed through as a driver URL parameter."""
        connection = {
            "dialect": "oracle+oracledb",
            "username": "user",
            "password": "pass",
            "host": "db.example.com",
            "port": "1521",
            "database": "PROD",
            "parameters": {"thick_mode": "true"},
        }
        accessor = object.__new__(ext_sql_alchemy.SQLAlchemyAccessor)
        accessor.sqlalchemy = Mock()

        accessor.make_engine(connection, echo=False)

        accessor.sqlalchemy.create_engine.assert_called_once_with(
            "oracle+oracledb://user:pass@db.example.com:1521?"
            "service_name=PROD",
            echo=False,
            thick_mode=True,
        )


class TestSQLAlchemyTemplate(unittest.TestCase):
    """test template features"""

    def setUp(self):
        self.accessor = ext_sql_alchemy.SQLAlchemyAccessor(parse(NORTHWIND))

    def test_find_variables(self):
        """test locating undeclared variables in the template"""
        s = """
        Select * from [Order]
        where
            CustomerId={{ cid }}
            and EmployeeId={{ eid }}
        """
        t = ext_sql_alchemy.SQLAlchemyTemplateSource(
            self.accessor,
            s
        )
        self.assertEqual(
            sorted(["eid", "cid"]),
            sorted(t.discover_parameters())
        )

    def test_no_vars(self):
        """test test that template work with no vars."""
        s = "Select * from [Order]"
        t = ext_sql_alchemy.SQLAlchemyTemplateSource(
            self.accessor,
            s
        )
        a = t.get_rendered_sql()
        self.assertEqual(
            a, s
        )

    def test_select_dict(self):
        s = """
        Select * from [Order]
        where
            CustomerId='{{ cid }}'
        """
        t = ext_sql_alchemy.SQLAlchemyTemplateSource(
            self.accessor,
            s
        )
        t["cid"] = "LAZYK"

        self.assertEqual(len(list(t)), 2)

    def test_select(self):
        s = """
        Select * from [Order]
        where
            CustomerId='{{ cid }}'
        """
        t = ext_sql_alchemy.SQLAlchemyTemplateSource(
            self.accessor,
            s,
            {"cid": "LAZYK"}
        )
        r = list(t)
        self.assertEqual(len(list(r)), 2)

    def test_select_raise(self):
        s = """
        Select * from [Order]
        where
            CustomerId='{{ cid }}'
        """
        t = ext_sql_alchemy.SQLAlchemyTemplateSource(
            self.accessor,
            s
        )
        with self.assertRaises(jinja2.exceptions.UndefinedError):
            _ = list(t)


class TestSQLAlchemyFactory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #
        # Impala is not a well integrated dialect
        #
        # impala and duckdb require third-party SA dialect packages
        cls.dialects = [
            i for i in ext_sql_alchemy.VALID_DIALECTS
            if i not in ("impala", "duckdb")
        ]

        cls.validator = schema.EntityValidator(
            yaml.load(SCHEMA, Loader=yaml.SafeLoader)
        )

    def test_sql_create(self):
        """
        Create SQL statement from entity
        """
        factory = ext_sql_alchemy.SQLAlchemyModelFactory()
        for dialect in self.dialects:
            if dialect not in ["hdf5", "awsathena+rest" ]:
                print(factory.create_sql_schema(dialect, person=self.validator))

    def test_sql_select(self):
        """
        Create SQL select statement from entity
        """
        factory = ext_sql_alchemy.SQLAlchemyModelFactory()
        for dialect in self.dialects:
            if dialect not in ["hdf5", "awsathena+rest"]:
                print(factory.create_sql_select(dialect, person=self.validator))


class TestSQLAlchemyBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.validator = schema.EntityValidator(
            yaml.load(SCHEMA, Loader=yaml.SafeLoader)
        )
        cls.table_name = "input"
        # cls.url = "sqlite:///input_files/sqlite.db"
        cls.url = "sqlite:///:memory:"
        cls.accessor = ext_sql_alchemy.SQLAlchemyAccessor(parse(cls.url), echo=False)

    def create_model(self):
        self.accessor.create_table(self.table_name, self.validator)

    def insert_data(self):
        the_sink = ext_sql_alchemy.SQLAlchemySink(self.accessor, self.table_name)
        the_sink.process(
            transform.CoerceTransform(self.validator)(iter(SAMPLE_DATA))
        )

    @classmethod
    def tearDownClass(cls):
        del cls.accessor


class TestSQLAlchemyReflection(TestSQLAlchemyBase):

    def test_reflect_entity(self):
        """reflect one entity"""
        self.create_model()
        self.insert_data()
        r = ext_sql_alchemy.SQLAlchemyReflector(self.accessor)
        e = r.reflect_entity("input")
        self.assertEqual(
            dict(e),
            {
                'id': 'String(primary_key=True, str_len=11)',
                'birthday': 'DateTime()',
                'company': 'String(str_len=32)',
                'ip': 'String(str_len=15)',
                'name': 'String(str_len=22)',
                'score': 'Float()',
                'year': 'Integer()'
            }
        )

    def _get_reflector(self) -> ext_sql_alchemy.SQLAlchemyReflector:
        accessor = ext_sql_alchemy.SQLAlchemyAccessor(
            parse(NORTHWIND),
            echo=False
        )
        return ext_sql_alchemy.SQLAlchemyReflector(accessor)

    def test_list_tables(self):
        """test table names reflection"""
        reflector = self._get_reflector()
        tables = reflector.get_table_names()
        self.assertEqual(
            tables,
            NORTHWIND_TABLE_NAMES
        )

    def test_profile(self):
        reflector = self._get_reflector()
        profile = reflector.extract_profile(*reflector.get_table_names())
        self.assertEqual(list(sorted(profile.keys())), NORTHWIND_TABLE_NAMES)
        # each entry must have a "schema" and a "relations" key
        for tbl, entry in profile.items():
            self.assertIn("schema", entry, msg=f"{tbl} missing schema")
            self.assertIn("relations", entry, msg=f"{tbl} missing relations")
        # spot-check Category columns (stable across SA versions)
        category_fields = set(profile["Category"]["schema"].keys())
        self.assertGreaterEqual(
            category_fields, {"Id", "CategoryName", "Description"}
        )


class TestSQLAlchemyCRUD(TestSQLAlchemyBase):
    """
    test create /insert / query operations
    """

    def test_0_model(self):
        """
        Test creating table from inferred model
        """
        self.create_model()

    def test_1_insert(self):
        """
        test writing data to tables
        """
        self.insert_data()

    def test_2_read_table(self):
        """
        test reading from tables
        """
        the_source = ext_sql_alchemy.SQLAlchemyTableSource(self.accessor, self.table_name)
        self.assertEqual(len(list(the_source)), SAMPLE_COUNT)

    def test_3_select(self):
        """
        test reading from tables
        """
        select_stmt = "select * from {}".format(self.table_name)
        the_source = ext_sql_alchemy.SQLAlchemySelectSource(
            self.accessor,
            select_stmt
        )
        self.assertEqual(len(list(the_source)), SAMPLE_COUNT)

    def test_4_inspect(self):
        """test inspect object"""
        self.assertEqual(
            self.accessor.inspect.get_table_names(),
            ["input"]
        )

    def test_5_execute(self):
        """test executing multiple queries"""
        # select_stmt = "select * from {}".format(self.table_name)
        select_stmt = "select * from input;"
        print(select_stmt)
        results = self.accessor.execute(select_stmt, multiple=True)
        for result in results:
            print(result)


class TestSQLAlchemyTableSourceSomeFields(TestSQLAlchemyBase):
    """
    Cover SQLAlchemyTableSource.iter_some_fields — previously untested path.

    Uses select(*fields).where(...) internally; this is the path most affected
    by the SA 2.0 migration (select-list syntax + whereclause kwarg removal).
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.create_model(cls())
        cls.insert_data(cls())

    def test_iter_some_fields_returns_subset(self):
        """iter_some_fields yields only the requested columns."""
        the_source = ext_sql_alchemy.SQLAlchemyTableSource(
            self.accessor, self.table_name
        )
        rows = list(the_source.iter_some_fields(["id", "year"]))
        self.assertEqual(len(rows), SAMPLE_COUNT)
        for row in rows:
            self.assertEqual(set(row.keys()), {"id", "year"})

    def test_iter_some_fields_with_where(self):
        """iter_some_fields respects a WHERE clause."""
        the_source = ext_sql_alchemy.SQLAlchemyTableSource(
            self.accessor,
            self.table_name,
            where_clause="year = 2020",
        )
        rows = list(the_source.iter_some_fields(["id", "year"]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["year"], 2020)

    def test_iter_some_fields_with_limit(self):
        """iter_some_fields respects the limit parameter."""
        the_source = ext_sql_alchemy.SQLAlchemyTableSource(
            self.accessor,
            self.table_name,
            limit=3,
        )
        rows = list(the_source.iter_some_fields(["id", "score"]))
        self.assertLessEqual(len(rows), 3)


class TestSQLAlchemySinkRoundTrip(unittest.TestCase):
    """
    Verify that SQLAlchemySink.process() commits rows that are immediately
    readable.  This covers the explicit-commit path required under SA 2.0.

    Each test method gets a fresh in-memory database so table creation never
    conflicts.
    """

    def setUp(self):
        self.validator = schema.EntityValidator(
            yaml.load(SCHEMA, Loader=yaml.SafeLoader)
        )
        self.table_name = "input"
        self.accessor = ext_sql_alchemy.SQLAlchemyAccessor(
            parse("sqlite:///:memory:"), echo=False
        )
        self.accessor.create_table(self.table_name, self.validator)
        sink = ext_sql_alchemy.SQLAlchemySink(self.accessor, self.table_name)
        sink.process(transform.CoerceTransform(self.validator)(iter(SAMPLE_DATA)))

    def tearDown(self):
        self.accessor.close()

    def test_sink_round_trip_count(self):
        """rows written by the sink are visible in a subsequent read."""
        the_source = ext_sql_alchemy.SQLAlchemyTableSource(
            self.accessor, self.table_name
        )
        self.assertEqual(len(list(the_source)), SAMPLE_COUNT)

    def test_sink_round_trip_values(self):
        """values survive the sink→source round trip without corruption."""
        select_stmt = f"select id, year, score from {self.table_name} order by id"
        rows = list(
            ext_sql_alchemy.SQLAlchemySelectSource(self.accessor, select_stmt)
        )
        self.assertEqual(len(rows), SAMPLE_COUNT)
        for i, row in enumerate(rows):
            self.assertEqual(row["year"], SAMPLE_DATA[i]["year"])


class TestSQLAlchemyDialect(unittest.TestCase):
    """Cover dialect loading and error handling in SQLAlchemyModelFactory."""

    @classmethod
    def setUpClass(cls):
        cls.factory = ext_sql_alchemy.SQLAlchemyModelFactory()
        cls.validator = schema.EntityValidator(
            yaml.load(SCHEMA, Loader=yaml.SafeLoader)
        )

    def test_valid_dialect_sqlite(self):
        """create_sql_schema succeeds for sqlite dialect."""
        sql = self.factory.create_sql_schema("sqlite", t=self.validator)
        self.assertIn("CREATE TABLE", sql)

    def test_valid_dialect_mysql(self):
        """create_sql_schema succeeds for mysql dialect."""
        sql = self.factory.create_sql_schema("mysql+mysqldb", t=self.validator)
        self.assertIn("CREATE TABLE", sql)

    def test_invalid_dialect_raises(self):
        """An unrecognised dialect raises DKitETLException."""
        with self.assertRaises(DKitETLException):
            self.factory.create_sql_schema("notadialect", t=self.validator)

    def test_select_sqlite(self):
        """create_sql_select succeeds for sqlite dialect."""
        sql = self.factory.create_sql_select("sqlite", t=self.validator)
        self.assertIn("SELECT", sql)


@unittest.skip("requires external model.yml — integration test only")
class TestSQLServices(unittest.TestCase):

    def test_sample_all(self):
        """test sampling all data from a database"""
        services = ext_sql_alchemy.SQLServices.from_file("model.yml")
        sample = services.sample_from_db("northwind")
        self.assertEqual(
            list(sample.keys()),
            NORTHWIND_TABLE_NAMES
        )

    def test_sample_specified(self):
        """test sampling all data from a database"""
        services = ext_sql_alchemy.SQLServices.from_file("model.yml")
        sample = services.sample_from_db("northwind", "Category", "Employee")
        self.assertEqual(
            list(sample.keys()),
            ["Category", "Employee"]
        )


if __name__ == '__main__':
    unittest.main()
