import unittest
import sys; sys.path.insert(0, "..")  # noqa
from dkit.etl.extensions.ext_athena import SchemaGenerator
from dkit.etl.model import Entity

person_entity = Entity(
    {
        "month_id": "Int()",
        "day_id": "Int()",
        "name": "String()",
        "float": "Float()",
        "decimal": "Decimal(precision=13, scale=2)",
    }
)
data_keys = ["name", "float", "decimal"]
partition_keys = ["month_id", "day_id"]

schema_genertor = SchemaGenerator(
    table_name="Person",
    entity=person_entity,
    partition_by=["month_id", "day_id"],
    kind="parquet",
    location="s3://dummy/folder"
)


class TestCase(unittest.TestCase):

    def test_get_ddl(self):
        test = schema_genertor.get_ddl()
        print(test)

    def test_get_repair_table(self):
        test = schema_genertor.get_repair_table()
        print(test)

    def test_data_fields(self):
        test = schema_genertor.data_fields()
        self.assertEqual(
            list(test),
            data_keys
        )

    def test_parition_fields(self):
        test = schema_genertor.partition_fields()
        self.assertEqual(
            list(test),
            partition_keys
        )


if __name__ == '__main__':
    unittest.main()
