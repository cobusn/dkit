# Copyright (c) 2024 Cobus Nel
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
Export model to python classes

=========== =============== =================================================
Jun 2024    Cobus Nel       Initial version
=========== =============== =================================================
"""
# from ...utilities.cmd_helper import LazyLoad
from jinja2 import Template
import logging
logger = logging.getLogger(__name__)


__all__ = []

# convert cannonical to python types

PB_TYPEMAP = {
    "float": "float",
    "double": "float",
    "integer": "int",
    "int8": "int",
    "int16": "int",
    "int32": "int",
    "int64": "int",
    "uint8": "int",
    "uint16": "int",
    "uint32": "int",
    "uint64": "int",
    "string": "str",
    "boolean": "bool",
    "binary": "bytes",
    "datetime": "datetime",
    "date": "date",
    "decimal": "Decimal",
}

str_template = """
{%- for import in imports %}
{{ import }}
{%- endfor %}
from dataclasses import dataclass
{%- for entity, typemap in entities.items() %}

# {{ entity }}
@dataclass
class {{ entity }}:
    {% for field, props in typemap.schema.items() -%}
    {{ field }}: {{ tm[props["type"]] }}
    {% endfor -%}
{%- endfor %}
"""


class DataClassSchemaGenerator(object):
    """
    Create .proto file from cannonical schema
    """

    def __init__(self, protocol=3, **entities):
        self.protocol = f"proto{protocol}"
        self.entities = entities
        self.type_map = PB_TYPEMAP

    def make_imports(self):
        """
        create imports
        """
        stmts = {
            "datetime": "from datetime import datetime",
            "date": "from date import date",
            "decimal": "from decimal imort Decimal",
        }
        types = set()
        for entity in self.entities.values():
            for field in entity.schema.values():
                types.add(field["type"])
        needed = set(stmts.keys()).intersection(types)
        return [stmts[k] for k in needed]

    def create_schema(self):
        """
        Create python code to define pyarrow schema
        """
        template = Template(str_template)
        return template.render(
            protocol=self.protocol,
            entities=self.entities,
            imports=self.make_imports(),
            tm=self.type_map,
            str=str
        )
