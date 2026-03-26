import datetime
import os
import sys; sys.path.insert(0, "..")  # noqa
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dkit.data.fake_helper import (
    persons, generate_data_rows, CANNONICAL_ROW_SCHEMA,
    generate_partition_rows, partition_data_schema
)
from dkit.etl import source
from dkit.etl.extensions.ext_arrow import (
    ArrowSchemaGenerator, ParquetSink, ParquetSource, build_table,
    infer_arrow_schema, infer_and_coerce_arrow_schema,
    auto_write_parquet, make_arrow_schema, make_partition_path,
    write_chunked_datasets, clear_partition_data
)
from dkit.etl.model import Entity
from dkit.etl.reader import FileReader
from dkit.etl.schema import EntityValidator
from dkit.etl.writer import FileWriter


TEST_DIR = Path(__file__).resolve().parent
DATA_DIR = TEST_DIR / "data"
OUTPUT_DIR = TEST_DIR / "output"
PARQUET_FILE = str(OUTPUT_DIR / "mtcars.parquet")
COVERAGE_FILE = str(DATA_DIR / "coverage.parquet")
AUTO_WRITE_FILE = str(DATA_DIR / "auto_write.parquet")
AUTO_WRITE_TYPED_FILE = str(DATA_DIR / "auto_write_typed.parquet")
COERCE_INFER_FILE = str(DATA_DIR / "coerce_infer.parquet")
COERCE_SCHEMA_FILE = str(DATA_DIR / "coerce_schema.parquet")
OPEN_WRITER_FILE = str(OUTPUT_DIR / "open_writer.parquet")


class OpenBinaryReader:
    is_open = True

    def __init__(self, file_obj):
        self.file_obj = file_obj

    def __getattr__(self, name):
        return getattr(self.file_obj, name)


class OpenBinaryWriter:
    is_open = True

    def __init__(self, file_obj):
        self.file_obj = file_obj

    def __getattr__(self, name):
        return getattr(self.file_obj, name)


def load_mtcars():
    with source.load(str(DATA_DIR / "mtcars.jsonl")) as infile:
        return list(infile)


def assert_rows_almost_equal(test_case, actual, expected, places=4):
    test_case.assertEqual(len(actual), len(expected))

    for i, (actual_row, expected_row) in enumerate(zip(actual, expected)):
        test_case.assertEqual(
            set(actual_row.keys()),
            set(expected_row.keys()),
            f"row {i} keys differ"
        )
        for key in expected_row:
            actual_value = actual_row[key]
            expected_value = expected_row[key]
            if isinstance(actual_value, float) and isinstance(expected_value, float):
                test_case.assertAlmostEqual(
                    actual_value,
                    expected_value,
                    places=places,
                    msg=f"row {i} field {key!r} differs"
                )
            else:
                test_case.assertEqual(
                    actual_value,
                    expected_value,
                    f"row {i} field {key!r} differs"
                )


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

    def test_schema(self):
        """generate executable PyArrow schema code for a simple entity"""
        g = ArrowSchemaGenerator(client=self.client)
        generated = g.create_schema()
        namespace = {}
        exec(generated, namespace)
        self.assertEqual(
            namespace["schema_client"],
            pa.schema(
                [
                    pa.field("name", pa.string()),
                    pa.field("surname", pa.string()),
                    pa.field("age", pa.int32()),
                ]
            )
        )


class TestPyArrowExtension(unittest.TestCase):

    def test_create_table_noschema(self):
        """build an Arrow table from row data without an explicit schema"""
        table = build_table(persons(10_001), micro_batch_size=1000)
        self.assertEqual(
            len(table),
            10_001
        )

    def test_create_table_types(self):
        """build an Arrow table that conforms to an explicit canonical schema"""
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

    def test_create_table_schema(self):
        """build an Arrow table from a hand-written person schema"""
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

    def test_infer_schema(self):
        """infer the expected Arrow schema from generated person records"""
        validate = pa.schema(
            [
                pa.field("last_name", pa.string()),
                pa.field("job", pa.string()),
                pa.field("birthday", pa.timestamp("us")),
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

    def test_infer_schema_does_not_coerce_rows(self):
        """infer schema only and leave source row values unchanged"""
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
                "day_id": "2024-01-02",
            }
        ]
        schema, data = infer_arrow_schema(rows)
        row = next(iter(data))

        self.assertEqual(schema.field("amount").type, pa.int32())
        self.assertEqual(schema.field("created_at").type, pa.timestamp("us"))
        self.assertEqual(schema.field("day_id").type, pa.date32())
        self.assertEqual(type(row["amount"]), str)
        self.assertEqual(type(row["created_at"]), str)
        self.assertEqual(type(row["day_id"]), str)

    def test_infer_and_coerce_schema_coerces_rows(self):
        """infer schema and coerce row values to the inferred Python types"""
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
                "day_id": "2024-01-02",
            }
        ]
        schema, data = infer_and_coerce_arrow_schema(rows)
        row = next(iter(data))

        self.assertEqual(schema.field("amount").type, pa.int32())
        self.assertEqual(schema.field("created_at").type, pa.timestamp("us"))
        self.assertEqual(schema.field("day_id").type, pa.date32())
        self.assertEqual(type(row["amount"]), int)
        self.assertEqual(type(row["created_at"]), datetime.datetime)
        self.assertEqual(type(row["day_id"]), datetime.datetime)

    def test_entity_call_with_explicit_schema(self):
        """coerce rows against an explicit entity schema without inference"""
        schema = Entity.from_encoded_dict(
            {
                "amount": "Integer()",
                "created_at": "DateTime()",
            }
        )
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
            }
        ]
        row = next(iter(schema(rows)))

        self.assertEqual(type(row["amount"]), int)
        self.assertEqual(type(row["created_at"]), datetime.datetime)


class TestDataTypesCoverage(unittest.TestCase):
    """make sure all data types are written and read correctly"""

    @classmethod
    def setUpClass(cls):
        cls.data = list(generate_data_rows())
        cls.schema = Entity(CANNONICAL_ROW_SCHEMA)

    def test_a_write(self):
        """write the coverage dataset to parquet with the canonical schema"""
        writer = FileWriter(COVERAGE_FILE, "wb")
        ParquetSink(writer, schema=self.schema).process(self.data)

    def test_b_read(self):
        """round-trip all supported coverage fields through parquet"""
        with source.load(COVERAGE_FILE) as infile:
            retrieved = list(infile)
            self.assertEqual(retrieved, self.data)

    def test_c_schema(self):
        """verify the written parquet schema preserves the expected Arrow types"""
        schema = pq.ParquetFile(COVERAGE_FILE).schema_arrow

        self.assertEqual(schema.field("binary").type, pa.binary())
        self.assertEqual(schema.field("datetime").type, pa.timestamp("us"))
        self.assertEqual(schema.field("date").type, pa.date32())
        self.assertEqual(schema.field("int8").type, pa.int8())
        self.assertEqual(schema.field("int16").type, pa.int16())
        self.assertEqual(schema.field("int32").type, pa.int32())
        self.assertEqual(schema.field("int64").type, pa.int64())


class TestAutoWriteParquet(unittest.TestCase):

    def test_auto_write_parquet_can_coerce_inferred_rows(self):
        """write stringly typed rows through auto inference with coercion enabled"""
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
                "day_id": "2024-01-02",
            }
        ]

        auto_write_parquet(AUTO_WRITE_FILE, rows, n=1, coerce=True)

        with source.load(AUTO_WRITE_FILE) as infile:
            retrieved = list(infile)

        self.assertEqual(retrieved[0]["amount"], 10)
        self.assertEqual(
            retrieved[0]["created_at"],
            datetime.datetime(2024, 1, 2, 3, 4, 5)
        )
        self.assertEqual(retrieved[0]["day_id"], datetime.date(2024, 1, 2))

    def test_auto_write_parquet_without_coercion_uses_typed_rows(self):
        """write already typed rows through auto inference without coercion"""
        rows = [
            {
                "amount": 10,
                "created_at": datetime.datetime(2024, 1, 2, 3, 4, 5),
                "day_id": datetime.date(2024, 1, 2),
            }
        ]

        auto_write_parquet(AUTO_WRITE_TYPED_FILE, rows, n=1, coerce=False)

        with source.load(AUTO_WRITE_TYPED_FILE) as infile:
            retrieved = list(infile)

        self.assertEqual(retrieved, rows)


class A_TestParquetSink(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mtcars = load_mtcars()

    def test_parquet_sink_auto_schema(self):
        """write the mtcars fixture to parquet using inferred schema"""
        w = FileWriter(PARQUET_FILE, "wb")
        snk = ParquetSink(w)
        snk.process(self.mtcars)

    def test_parquet_sink_rejects_field_names(self):
        """reject field selection on the parquet sink constructor"""
        with self.assertRaises(NotImplementedError):
            ParquetSink(FileWriter(PARQUET_FILE, "wb"), field_names=["disp"])

    def test_parquet_sink_auto_schema_with_coercion(self):
        """write rows with inferred schema and explicit coercion enabled"""
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
                "day_id": "2024-01-02",
            }
        ]
        ParquetSink(FileWriter(COERCE_INFER_FILE, "wb"), coerce=True).process(rows)

        with source.load(COERCE_INFER_FILE) as infile:
            retrieved = list(infile)

        self.assertEqual(retrieved[0]["amount"], 10)
        self.assertEqual(
            retrieved[0]["created_at"],
            datetime.datetime(2024, 1, 2, 3, 4, 5)
        )
        self.assertEqual(retrieved[0]["day_id"], datetime.date(2024, 1, 2))

    def test_parquet_sink_explicit_schema_with_coercion(self):
        """write rows with an explicit schema and coercion enabled"""
        schema = Entity.from_encoded_dict(
            {
                "amount": "Integer()",
                "created_at": "DateTime()",
            }
        )
        rows = [
            {
                "amount": "10",
                "created_at": "2024-01-02 03:04:05",
            }
        ]
        ParquetSink(FileWriter(COERCE_SCHEMA_FILE, "wb"), schema=schema, coerce=True).process(rows)

        with source.load(COERCE_SCHEMA_FILE) as infile:
            retrieved = list(infile)

        self.assertEqual(retrieved[0]["amount"], 10)
        self.assertEqual(
            retrieved[0]["created_at"],
            datetime.datetime(2024, 1, 2, 3, 4, 5)
        )

    def test_parquet_sink_with_open_writer(self):
        """write parquet rows through the already-open writer branch"""
        with open(OPEN_WRITER_FILE, "wb") as outfile:
            writer = OpenBinaryWriter(outfile)
            ParquetSink(writer).process(self.mtcars)

        with source.load(OPEN_WRITER_FILE) as infile:
            data = list(infile)
        assert_rows_almost_equal(self, data, self.mtcars)


class B_TestParquetSource(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.mtcars = load_mtcars()
        writer = FileWriter(PARQUET_FILE, "wb")
        ParquetSink(writer).process(cls.mtcars)

    def assertRowsAlmostEqual(self, actual, expected, places=4):
        assert_rows_almost_equal(self, actual, expected, places=places)

    def test_parquet_source(self):
        """read the mtcars parquet file and compare rows with float tolerance"""
        r = FileReader(PARQUET_FILE, "rb")
        src = ParquetSource([r])
        data = list(src)
        self.assertRowsAlmostEqual(
            data,
            self.mtcars
        )

    def test_parquet_source_some_fields(self):
        """read only selected parquet columns from the mtcars fixture"""
        r = FileReader(PARQUET_FILE, "rb")
        src = ParquetSource([r], field_names=["disp", "drat"])
        data = list(src)
        rows = [
            {k: row[k] for k in ["disp", "drat"]}
            for row in self.mtcars
        ]
        self.assertEqual(data, rows)

    def test_parquet_source_with_open_reader(self):
        """read parquet rows through the already-open reader branches"""
        with open(PARQUET_FILE, "rb") as infile:
            src = ParquetSource([OpenBinaryReader(infile)])
            data = list(src)
        self.assertRowsAlmostEqual(data, self.mtcars)

        with open(PARQUET_FILE, "rb") as infile:
            src = ParquetSource([OpenBinaryReader(infile)], field_names=["disp", "drat"])
            data = list(src)
        rows = [{k: row[k] for k in ["disp", "drat"]} for row in self.mtcars]
        self.assertEqual(data, rows)


class TestDataSets(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.path = str(DATA_DIR / "month_id=20230101" / "day_id=20230101")
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
        """build partition paths for local and S3-style base paths"""
        self.assertEqual(
            make_partition_path(self.partitions, self.td, "."),
            "./month_id=20231101/day_id=20231104"
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
        """raise when a required partition key is missing"""
        td = {
            "month_id": 20231101,
        }
        with self.assertRaises(KeyError) as _:
            make_partition_path(self.partitions, td, ".")

    def test_make_partition_path_null(self):
        """raise when no base path or partition list is supplied"""
        td = {}
        with self.assertRaises(ValueError) as _:
            make_partition_path([], td)

    def test_a_make_partitioned_dataset(self):
        """write a partitioned dataset to the local test data directory"""
        schema = make_arrow_schema(partition_data_schema)
        write_chunked_datasets(
            generate_partition_rows(1000),
            str(DATA_DIR),
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
        """delete the contents of an existing partition folder"""
        clear_partition_data(
            None,
            ["month_id", "day_id"],
            {"month_id": 20230101, "day_id": 20230101},
            str(DATA_DIR)
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
            str(DATA_DIR)
        )


if __name__ == '__main__':
    unittest.main()
