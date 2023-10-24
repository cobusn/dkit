import sys; sys.path.insert(0, "..")  # noqa
import unittest
from zlib import adler32
import os
import pyarrow as pa

from dkit.data.fake_helper import (
    persons, generate_data_rows, CANNONICAL_ROW_SCHEMA,
    generate_partition_rows, partition_data_schema
)
from dkit.etl import source
from dkit.etl.extensions.ext_arrow import (
    ArrowSchemaGenerator, ParquetSink, ParquetSource, build_table,
    infer_arrow_schema, make_arrow_schema, make_partition_path,
    write_chunked_datasets, clear_partition_data
)
from dkit.etl.model import Entity
from dkit.etl.reader import FileReader
from dkit.etl.schema import EntityValidator
from dkit.etl.writer import FileWriter


PARQUET_FILE = "output/mtcars.parquet"
with source.load("data/mtcars.jsonl") as infile:
    MTCARS = list(infile)


class TestPyArrowSchemaExport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = EntityValidator(
            {
                "name": {"type": "string"},
                "surname": {"type": "string"},
                "age": {"type": "integer"},
            }
        )

    def __test_schema(self):
        g = ArrowSchemaGenerator(client=self.client)
        h = adler32(g.create_schema().encode())
        self.assertTrue(h in (3140830570,))


class TestPyArrowExtension(unittest.TestCase):

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
            generate_data_rows(1000),
            schema=arrow_schema,
            micro_batch_size=100
        )
        self.assertEqual(
            len(table),
            1000
        )

    def __test_create_table_schema(self):
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

    def __test_schema(self):
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


class A_TestParquetSink(unittest.TestCase):

    def __test_parquet_sink_auto_schema(self):
        """test writing to parquet with auto generated schema"""
        w = FileWriter(PARQUET_FILE, "wb")
        snk = ParquetSink(w)
        snk.process(MTCARS)


class B_TestParquetSource(unittest.TestCase):

    def __test_parquet_source(self):
        """test writing to parquet with auto generated schema"""
        r = FileReader(PARQUET_FILE, "rb")
        src = ParquetSource([r])
        data = list(src)
        self.assertEqual(
            data,
            MTCARS
        )

    def __test_parquet_source_some_fields(self):
        """test writing to parquet with auto generated schema"""
        r = FileReader(PARQUET_FILE, "rb")
        src = ParquetSource([r], field_names=["disp", "drat"])
        data = list(src)
        rows = [
            {k: row[k] for k in ["disp", "drat"]}
            for row in MTCARS
        ]
        self.assertEqual(data, rows)


class TestDataSets(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.path = "data/month_id=20230101/day_id=20230101/"
        if os.path.exists(cls.path):
            for file in os.listdir(cls.path):
                file_path = os.path.join(cls.path, file)
                os.remove(file_path)

    def setUp(self):
        self.td = {
            "month_id": 20231101,
            "day_id": 20231104,
        }
        self.partitions = list(self.td.keys())

    def test_make_partition_path(self):
        self.assertEqual(
            make_partition_path(self.partitions, self.td),
            "month_id=20231101/day_id=20231104"
        )
        self.assertEqual(
            make_partition_path(self.partitions, self.td, "s3://bucket"),
            "s3://bucket/month_id=20231101/day_id=20231104"
        )
        self.assertEqual(
            make_partition_path(self.partitions, self.td, "s3://bucket/"),
            "s3://bucket/month_id=20231101/day_id=20231104"
        )

    def test_make_partition_path_err(self):
        td = {
            "month_id": 20231101,
        }
        with self.assertRaises(KeyError) as _:
            make_partition_path(self.partitions, td)

    def test_make_partition_path_null(self):
        td = {}
        with self.assertRaises(ValueError) as _:
            make_partition_path([], td)

    def test_a_make_partitioned_dataset(self):
        schema = make_arrow_schema(partition_data_schema)
        write_chunked_datasets(
            generate_partition_rows(1000),
            "data",
            schema,
            ["month_id", "day_id"],
            None,
            100,
            existing_data_behaviour="overwrite_or_ignore"
        )
        self.assertTrue(
            len(os.listdir(self.path)) > 0
        )

    def test_b_clean_partitioned_folder(self):
        clear_partition_data(
            None,
            ["month_id", "day_id"],
            {"month_id": 20230101, "day_id": 20230101},
            "data"
        )
        self.assertEqual(
            len(os.listdir(self.path)),
            0
        )

    def test_c_clean_partitioned_folder(self):
        """the below test for a partition that does not exist

        should complete without error as FileNotFound error is
        caught and ignored in this use case.
        """
        clear_partition_data(
            None,
            ["month_id", "day_id"],
            {"month_id": 20230101, "day_id": 20230109},
            "data"
        )


if __name__ == '__main__':
    unittest.main()
