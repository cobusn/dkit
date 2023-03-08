# Copyright (c) 2023 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


"""
Routines to interface with AWS Athena:

    - SQL Create table script
"""

from ..model import Entity
from jinja2 import Template
from datetime import date
from typing import List

str_template = """
--
-- {{ table_name }}
--
CREATE EXTERNAL TABLE IF NOT EXISTS `{{ table_name }}` (
{%- for field, props in c.items() %}
    `{{ field }}` {{ tm[props["type"]](props) }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- if len(partitions) > 0 %}
PARTITIONED BY (
{%- for field, props in partitions.items() %}
    `{{ field }}` {{ tm[props["type"]](props) }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- endif %}
STORED AS {{ kind | upper }}
LOCATION '{{ location }}'
{%- if properties %}
TBLPROPERTIES (
{%- for k, v in properties.items() %}
    '{{ k }}'='{{ v }}'
{%- endfor %}
)
{%- endif %}
;
"""


class SchemaGenerator(object):
    """
    {"parquet.compression": "SNAPPY"}
    """
    typemap = {
        "boolean": lambda t: "BOOLEAN",
        "binary": lambda t: "BINARY",
        "date": lambda t: "DATE",
        "datetime": lambda t: "TIMESTAMP",
        "decimal": lambda t: f"DECIMAL({t['precision']}, {t['scale']})",
        "float": lambda t: "FLOAT",
        "double": lambda t: "DOUBLE",
        "integer": lambda t: "INT",
        "int8": lambda t: "TINYINT",
        "int16": lambda t: "SMALLINT",
        "int32": lambda t: "INT",
        "int64": lambda t: "BIGINT",
        "string": lambda t: "STRING",
    }

    def __init__(
        self, table_name: str, entity: Entity, partition_by: List[str] = None,
        kind="parquet", location="s3://bucket/folder",
        properties=None,
    ):
        self.table_name = table_name
        self.entity = entity
        self.partition_by = partition_by if partition_by else []
        self.kind = kind
        self.location = location
        self.properties = properties

    def data_fields(self):
        """schema for data fields

        schema exclude fields used for partitioning
        """
        return {
            k: v
            for k, v in self.entity.as_entity_validator().schema.items()
            if k not in self.partition_by
        }

    def partition_fields(self):
        return {
            k: v
            for k, v in self.entity.as_entity_validator().schema.items()
            if k in self.partition_by
        }

    def create_schema(self):
        """
        Create python code to define spark schema
        """
        template = Template(str_template)
        return template.render(
            table_name=self.table_name,
            tm=self.typemap,
            c=self.data_fields(),
            # c=self.entity.as_entity_validator(),
            partitions=self.partition_fields(),
            timestamp=str(date.today()),
            kind=self.kind,
            location=self.location,
            properties=self.properties,
            len=len,
        )
