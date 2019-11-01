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
import unittest
import os
import sys
sys.path.insert(0, "..") # noqa
from dkit.etl.extensions import ext_tables
from dkit.etl import (reader, transform, source, schema)


class TestParsePath(unittest.TestCase):

    def test_parse1(self):
        uri = "/test"
        path, node = ext_tables.parse_fullpath(uri)
        self.assertEqual(path, "/")
        self.assertEqual(node, "test")

    def test_parse2(self):
        uri = "/test"
        path, node = ext_tables.parse_fullpath(uri)
        self.assertEqual(path, "/")
        self.assertEqual(node, "test")

    def test_parse3(self):
        uri = "/root/root/test"
        path, node = ext_tables.parse_fullpath(uri)
        self.assertEqual(path, "/root/root")
        self.assertEqual(node, "test")


class TestExtPyTables(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.filename = os.path.join("output", "sample.h5")
        super(TestExtPyTables, cls).setUpClass()
        cls.accessor = ext_tables.PyTablesAccessor(cls.filename, mode="w")

        # create validator and model
        the_iterable = source.JsonlSource([reader.FileReader(
            os.path.join("input_files", "sample.jsonl")
        )])
        cls.validator = schema.EntityValidator.from_iterable(the_iterable)

    def test_0_model(self):
        """
        Test creating table from inferred model
        """
        self.accessor.create_table("/tests/sample", self.validator, "Sample data")

    def test_1_write(self):
        """
        test writing data to tables
        """
        the_iterable = source.JsonlSource([reader.FileReader(os.path.join("input_files",
                                                                          "sample.jsonl"))])
        the_sink = ext_tables.PyTablesSink(self.accessor, "/tests/sample")
        the_sink.process(transform.CoerceTransform(self.validator)(the_iterable))

    def test_2_read(self):
        """
        test reading from tables
        """
        the_source = ext_tables.PyTablesSource(self.accessor, "/tests/sample")
        self.assertGreater(len(list(the_source)), 0)

    def test_3_nrows(self):
        the_source = ext_tables.PyTablesSource(self.accessor, "/tests/sample")
        self.assertGreater(the_source.nrows, 0)

    def test_4_get_tables(self):
        nodes = list(self.accessor.get_node_info())
        self.assertEqual(len(nodes), 3)
        self.assertEqual(
            nodes[2],
            {'type': 'Table', 'name': 'sample', 'path': '/tests/sample', 'size': 500}
        )

    def test_5_get_node_schema(self):
        e = self.accessor.get_node_schema("/tests/sample")
        # make sure this is parsed
        e.as_entity_validator()

    @classmethod
    def tearDownClass(cls):
        super(TestExtPyTables, cls).tearDownClass()
        cls.accessor.close()


if __name__ == '__main__':
    unittest.main()
