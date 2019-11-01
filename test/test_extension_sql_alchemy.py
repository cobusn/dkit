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
import os
import yaml
from dkit.etl.extensions import ext_sql_alchemy
from dkit.etl import (reader, source, schema, transform)


SCHEMA = """
id: {str_len: 11, type: string, primary_key: True}
birthday: {type: datetime}
company: {str_len: 32, type: string}
ip: {str_len: 15, type: string}
name: {str_len: 22, type: string}
score: {type: float}
year: {type: integer}
"""


class TestSQLAlchemyFactory(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.validator = schema.EntityValidator(
            yaml.load(SCHEMA, Loader=yaml.SafeLoader)
        )

    def test_sql_create(self):
        """
        Create SQL statement from entity
        """
        factory = ext_sql_alchemy.SQLAlchemyModelFactory()
        for dialect in ext_sql_alchemy.VALID_DIALECTS:
            print(factory.create_sql_schema(dialect, person=self.validator))

    def test_sql_select(self):
        """
        Create SQL select statement from entity
        """
        factory = ext_sql_alchemy.SQLAlchemyModelFactory()
        for dialect in ext_sql_alchemy.VALID_DIALECTS:
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
        cls.accessor = ext_sql_alchemy.SQLAlchemyAccessor(cls.url, echo=False)

    def create_model(self):
        self.accessor.create_table(self.table_name, self.validator)

    def insert_data(self):
        the_iterable = source.JsonlSource(
            [reader.FileReader(os.path.join("input_files", "sample.jsonl"))]
        )
        the_sink = ext_sql_alchemy.SQLAlchemySink(self.accessor, self.table_name)

        the_sink.process(transform.CoerceTransform(self.validator)(the_iterable))

    @classmethod
    def tearDownClass(cls):
        del cls.accessor


class TestSQLAlchemyReflection(TestSQLAlchemyBase):

    def test_reflect_entity(self):
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


class TestSQLAlchemyExtension(TestSQLAlchemyBase):

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
        self.assertEqual(len(list(the_source)), 500)

    def test_3_select(self):
        """
        test reading from tables
        """
        select_stmt = "select * from {}".format(self.table_name)
        the_source = ext_sql_alchemy.SQLAlchemySelectSource(
            self.accessor,
            select_stmt
        )
        self.assertEqual(len(list(the_source)), 500)

    def test_4_inspect(self):
        """test inspect object"""
        self.assertEqual(
            self.accessor.inspect.get_table_names(),
            ["input"]
        )


if __name__ == '__main__':
    unittest.main()
