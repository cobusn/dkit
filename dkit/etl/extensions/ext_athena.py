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
from typing import List, Dict
from os.path import join
from urllib.parse import quote

_create_template = """
--
-- {{ table_name }}
--
CREATE EXTERNAL TABLE IF NOT EXISTS `{{ table_name }}` (
{%- for field, props in c.items() %}
    `{{ field }}` {{ tmap[props["type"]](props) }}{{ "," if not loop.last }}
{%- endfor %}
)
{%- if len(partitions) > 0 %}
PARTITIONED BY (
{%- for field, props in partitions.items() %}
     `{{ field }}` {{ tmap[props["type"]](props) }}{{ "," if not loop.last }}
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


_repair_partitions_tmplate = "MSCK REPAIR TABLE {{ table_name }}"

athena_typemap = {
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


class SchemaGenerator(object):
    """
    {"parquet.compression": "SNAPPY"}
    """

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
        self.properties = properties if properties else {}

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
        """fields used for partitioning"""
        return {
            k: v
            for k, v in self.entity.as_entity_validator().schema.items()
            if k in self.partition_by
        }

    def get_create_sql(self):
        """Generate DDL to create Athena table"""
        template = Template(_create_template)
        return template.render(
            table_name=self.table_name,
            tmap=athena_typemap,
            c=self.data_fields(),
            partitions=self.partition_fields(),
            timestamp=str(date.today()),
            kind=self.kind,
            location=self.location,
            properties=self.properties,
            len=len
        )

    def get_repair_table_sql(self):
        """SQL source to read all partitions"""
        template = Template(_repair_partitions_tmplate)
        return template.render(
            table_name=self.table_name,
        )

    def get_partition_path(self, record: Dict) -> str:
        """calculate path with partition for record provided

        The function will extract the defined partition fields
        from the Dict.  Strings will be URL Quoted
        """
        part_fields = {
            k: record[k]
            for k in self.partition_by
        }
        paths = "/".join(
            f"{k}={quote(str(v))}"
            for k, v in part_fields.items()
        )
        return join(self.location, paths)
