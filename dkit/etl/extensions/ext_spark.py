# Copyright (c) 2019 Cobus Nel
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
Routines to interface with spark:

    - Crate Spark schemas from a model
"""

from jinja2 import Template


TYPEMAP = {
    "boolean": "BooleanType",
    "binary": "BinaryType",
    "date": "DateType",
    "datetime": "TimestampType",
    "decimal": "DecimalType",
    "float": "FloatType",
    "double": "Doubleype",
    "integer": "IntegerType",
    "int8": "ByteType",
    "int16": "ShortType",
    "int32": "IntegerType",
    "int64": "LongType",
    # Note Unsigned is not supported in Spark.
    # NB: unsigned types is casted up..
    "uint8": "ShortType",
    "uint16": "IntegerType",
    "uint32": "LongType",
    "uint64": "LongType",
    "string": "StringType",
}


str_template = """from pyspark.sql import types
from pyspark.sql.types import (
    StructType,
    {% for import in imports -%}
    {{ import }}{{ "," if not loop.last }}
    {% endfor -%}
)

{% for entity, typemap in entities.items() %}

# {{ entity }}
schema_{{ entity }} = StructType(
    [
        {% for field, props in typemap.schema.items() -%}
        types.StructField("{{ field }}", {{ tm[props["type"]] }}(), True),
        {% endfor -%}
    ]
)
{%- endfor %}

entity_map = {
{%- for entity in entities.keys() %}
    "{{ entity }}": schema_{{ entity }},
{%- endfor %}
}

"""


class SchemaGenerator(object):

    typemap = TYPEMAP

    def __init__(self, **entities):
        self.__entities = entities

    @property
    def entities(self):
        """
        dictionary of entities
        """
        return self.__entities

    @property
    def imports(self):
        return set(
            [
                self.typemap[i["type"]]
                for s in self.entities.values()
                for i in s.schema.values()
            ]
        )

    def create_schema(self):
        """
        Create python code to define spark schema
        """
        template = Template(str_template)
        return template.render(
            entities=self.entities,
            tm=self.typemap,
            imports=self.imports
        )


if __name__ == "__main__":
    from dkit.etl.schema import EntityValidator
    client = EntityValidator(
        {
            "name": {"type": "string"},
            "surname": {"type": "string"},
            "age": {"type": "integer"},
        }
    )

    g = SchemaGenerator(client=client)
    print(g.create_schema())
