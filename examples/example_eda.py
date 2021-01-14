"""Example usage of eda (Exploratory Data Analysis)"""
import sys; sys.path.insert(0, "..")  # noqa
from dkit.data.eda import SchemaMap
from dkit.etl import source

schema_map = SchemaMap(depth=800)

with source.load("data/runstats.jsonl.xz") as data:
    result = schema_map(data)

print(result)
