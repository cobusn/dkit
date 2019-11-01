import sys; sys.path.insert(0, "..") # noqa
import os
from dkit.etl.extensions import ext_tables
from dkit.etl import (reader, source, schema, transform)
from dkit.utilities.file_helper import yaml_load

# note this schema is in cerberus format
the_schema = """
id: {str_len: 11, type: string, primary_key: True}
birthday: {type: datetime}
company: {str_len: 32, type: string, index: True}
ip: {str_len: 15, type: string}
name: {str_len: 22, type: string}
score: {type: float}
year: {type: integer}
"""
full_path = "/input"


validator = schema.EntityValidator(yaml_load(the_schema))
accessor = ext_tables.PyTablesAccessor("test.h5")

# Create database from model
accessor.create_table(full_path, validator, title="input data")

# Create index
accessor.create_index(full_path, "year")

# Insert records into database
the_iterable = source.JsonlSource(
    [reader.FileReader(os.path.join("..", "test", "input_files", "sample.jsonl"))]
)
the_sink = ext_tables.PyTablesSink(accessor, "/input")

the_sink.process(transform.CoerceTransform(validator)(the_iterable))

# Query Database
the_source = ext_tables.PyTablesSource(accessor, full_path)
print(len(list(the_source)))

# Query using where clause
where_stmt = "year > 1972"
the_source = ext_tables.PyTablesSource(accessor, full_path, where_stmt)
print(len(list(the_source)))

accessor.close()
os.unlink("test.h5")
