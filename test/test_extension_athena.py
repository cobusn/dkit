import unittest
import sys; sys.path.insert(0, "..")  # noqa
from dkit.etl.extensions.ext_athena import SchemaGenerator
from dkit.etl.model import Entity
from decimal import Decimal


FILE_PATH = "s3://bucket/folder"

person_entity = Entity(
    {
        "name": "String()",
        "float": "Float()",
        "decimal": "Decimal(precision=13, scale=2)",
        "month_id": "Integer()",
        "day_id": "Integer()",
    }
)

instance = {
    "name": "Joe Dolan",
    "float": 20.1,
    "decimal": Decimal(10),
    "month_id": 202301,
    "day_id": 20230101
}

data_keys = ["name", "float", "decimal"]
partition_keys = ["month_id", "day_id"]


verify_ddl = """
--
-- Person
--
CREATE EXTERNAL TABLE IF NOT EXISTS `Person` (
    `name` STRING,
    `float` FLOAT,
    `decimal` DECIMAL(13, 2)
)
PARTITIONED BY (
     `month_id` INT,
     `day_id` INT
)
STORED AS PARQUET
LOCATION 's3://bucket/folder'
;"""


class TestCase(unittest.TestCase):

    def setUp(self):
        self.SchemaGenerator = SchemaGenerator(
            table_name="Person",
            entity=person_entity,
            partition_by=["month_id", "day_id"],
            kind="parquet",
            location=FILE_PATH,
        )

    def test_get_ddl(self):
        test = self.SchemaGenerator.get_create_sql()
        self.assertEqual(
            test, verify_ddl
        )

    def test_get_repair_table(self):
        test = self.SchemaGenerator.get_repair_table_sql()
        self.assertEqual(
            test,
            "MSCK REPAIR TABLE Person"
        )

    def test_data_fields(self):
        test = self.SchemaGenerator.data_fields()
        self.assertEqual(
            list(test),
            data_keys
        )

    def test_partition_fields(self):
        test = self.SchemaGenerator.partition_fields()
        self.assertEqual(
            list(test),
            partition_keys
        )

    def test_partition_path(self):
        check = "s3://bucket/folder/month_id=202301/day_id=20230101"
        test = self.SchemaGenerator.get_partition_path(instance)
        self.assertEqual(
            test,
            check
        )

        # Check that an additonal forward slash is handled
        self.SchemaGenerator.location = self.SchemaGenerator.location + "/"
        test = self.SchemaGenerator.get_partition_path(instance)
        self.assertEqual(
            test,
            check
        )

        # Check for values that need qoting
        this = {
            "month_id": "a b",
            "day_id": "a%b",
        }
        check = "s3://bucket/folder/month_id=a%20b/day_id=a%25b"
        test = self.SchemaGenerator.get_partition_path(this)
        self.assertEqual(
            test,
            check
        )


if __name__ == '__main__':
    unittest.main()
