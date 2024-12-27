import sys; sys.path.insert(0, "..") # noqa
import os
from dkit.etl.extensions import ext_sql_alchemy
from dkit.etl import (reader, source, schema, transform)
from dkit.utilities.file_helper import yaml_load
from dkit.etl.model import Connection


the_schema = """
id: {str_len: 11, type: string, primary_key: True}
birthday: {type: datetime}
company: {str_len: 32, type: string}
ip: {str_len: 15, type: string}
name: {str_len: 22, type: string}
score: {type: float}
year: {type: integer, index: True}
"""

validator = schema.EntityValidator(yaml_load(the_schema))
table_name = "input"
conn = Connection(
    dialect="sqlite",
    database="test.db",
    driver="sqlite"
)
accessor = ext_sql_alchemy.SQLAlchemyAccessor(conn.as_dict(), echo=True)

# Create database from model
accessor.create_table(table_name, validator)

# Insert records into database
the_iterable = source.JsonlSource(
    [reader.FileReader(os.path.join("..", "test", "input_files", "sample.jsonl"))]
)
the_sink = ext_sql_alchemy.SQLAlchemySink(accessor, table_name, chunk_size=13)
the_sink.process(transform.CoerceTransform(validator)(the_iterable))

# Query Database
the_source = ext_sql_alchemy.SQLAlchemyTableSource(accessor, table_name)
print(len(list(the_source)))

# Query using SQLAlchemySelectSource
select_stmt = "select * from {}".format(table_name)
the_source = ext_sql_alchemy.SQLAlchemySelectSource(accessor, select_stmt)
print(len(list(the_source)))
