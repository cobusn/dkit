import unittest
import sys; sys.path.insert(0, "..")  # noqa

from dkit.etl.extensions.ext_arrow import build_table, infer_arrow_schema, make_arrow_schema
from dkit.data.fake_helper import persons, generate_test_rows, CANNONICAL_ROW_SCHEMA
import pyarrow as pa
from dkit.etl.model import Entity


class TestCase(unittest.TestCase):

    def test_create_table_noschema(self):
        """create table from data"""
        table = build_table(persons(10_001), micro_batch_size=1000)
        self.assertEqual(
            len(table),
            10_001
        )

    def test_create_table_types(self):
        """create table from data"""
        arrow_schema = make_arrow_schema(Entity(CANNONICAL_ROW_SCHEMA))
        table = build_table(
            generate_test_rows(1000),
            schema=arrow_schema,
            micro_batch_size=100
        )
        self.assertEqual(
            len(table),
            1000
        )

    def test_create_table_schema(self):
        """create table from data"""
        cannonical = {
            'last_name': 'String(str_len=8)',
            'job': 'String(str_len=44)',
            'birthday': 'DateTime()',
            'first_name': 'String(str_len=9)',
            'gender': 'String(str_len=6)'
        }
        arrow_schema = make_arrow_schema(Entity(cannonical))
        table = build_table(
            persons(10_001),
            schema=arrow_schema,
            micro_batch_size=1000
        )
        self.assertEqual(
            len(table),
            10_001
        )

    def test_schema(self):
        """test create schema"""
        validate = pa.schema(
            [
                pa.field("last_name", pa.string()),
                pa.field("job", pa.string()),
                pa.field("birthday", pa.timestamp("s")),
                pa.field("first_name", pa.string()),
                pa.field("gender", pa.string())
            ]
        )
        schema, i = infer_arrow_schema(
            persons(100)
        )
        self.assertEqual(
            schema,
            validate
        )
        self.assertEqual(
            len(list(i)),
            100
        )


if __name__ == '__main__':
    unittest.main()
