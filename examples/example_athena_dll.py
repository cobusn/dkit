"""
Create Athena DLL from an Entity
"""

from dkit.etl.extensions.ext_athena import SchemaGenerator
from dkit.etl.model import Entity


if __name__ == "__main__":
    schema = {
        "name": "String()",
        "surname": "String()",
        "age": "Integer()",
        "value": "Decimal(precision=20, scale=3)",
        "month_id": "Integer()",
        "day_id": "Integer()",
    }
    partitions = ["month_id", "day_id"]
    client = Entity(schema)
    g = SchemaGenerator(
        table_name="Client",
        entity=client,
        partition_by=partitions,
        kind="parquet",
        location="s3://bucket/location",
        properties={"parquet.compression": "SNAPPY"},
    )
    print(g.create_schema())
