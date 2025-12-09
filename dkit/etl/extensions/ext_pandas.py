# Copyright (c) 2025 Cobus Nel
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
Extension for pyarrow

=========== =============== =================================================
Dec 2025    Cobus Nel       Initial version
=========== =============== =================================================
"""
import logging
import textwrap
from jinja2 import Template

logger = logging.getLogger("ext_pandas")


__all__ = []


PANDAS_TYPEMAP = {
    "float": "Float32",
    "double": "Float64",
    "integer": "Int64",
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "uint8": "Int16",   # uint is not nullable
    "uint16": "Int32",
    "uint32": "Int64",
    "uint64": "Int64",
    "string": "string",
    "boolean": "boolean",
    "binary": "object",
    "datetime": "datetime64[ns]",
    "date": "datetime64[ns]",
    "decimal": "object",
}


str_template = textwrap.dedent("""\
import pandas as pd
{%- for entity, typemap in entities.items() %}
# {{ entity }}
schema_{{ entity }} = {
{%- for field, typ in typemap.schema.items() %}
    {{ field }}: '{{ tm[typ['type']] }}',
{%- endfor %}
}
{% endfor %}
entity_map = {
{%- for entity in entities.keys() %}
    "{{ entity }}": schema_{{ entity }},
{%- endfor %}
}
""")


class PandasSchemaGenerator(object):
    """
    Create .py file that define pyarrow schema fromm
    cannonical entity schema (s).
    """

    def __init__(self, **entities):
        self.__entities = entities
        self.type_map = PANDAS_TYPEMAP

    @property
    def entities(self):
        """
        dictionary of entities
        """
        return self.__entities

    def create_schema(self):
        """
        Create python code to define pyarrow schema
        """
        template = Template(str_template)
        return template.render(
            entities=self.entities,
            tm=self.type_map,
            str=str
        )
