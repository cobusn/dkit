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
Extension and utilities for protobuf

=========== =============== =================================================
Aug 2023    Cobus Nel       Initial version
=========== =============== =================================================
"""
# from ...utilities.cmd_helper import LazyLoad
from jinja2 import Template
import logging
from typing import Dict
logger = logging.getLogger("ext_protobuf")


__all__ = []

# convert cannonical to arrow


PB_TYPEMAP = {
    "float": "float",
    "double": "double",
    "integer": "int32",
    "int8": "int32",
    "int16": "int32",
    "int32": "int32",
    "int64": "int64",
    "uint8": "uint32",
    "uint16": "uint32",
    "uint32": "uint32",
    "uint64": "uint64",
    "string": "string",
    "boolean": "bool",
    "binary": "bytes",
    "datetime": "google.protobuf.Timestamp",
    "date": "google.protobuf.Timestamp",          # No date specific type
    "decimal": "double",
}


str_template = """
syntax = "{{ protocol }}";

{%- for entity, typemap in entities.items() %}

// {{ entity }}
message {{ entity }} {
  {% for field, props in typemap.schema.items() -%}
  {{ props["type"] }} {{ field }} = {{ loop.index }};
  {% endfor -%}
}

{%- endfor %}
"""


class SchemaGenerator(object):
    """
    Create .proto file from cannonical schema
    """

    def __init__(self, protocol=3, **entities):
        self.protocol = f"proto{protocol}"
        self.entities = entities
        self.type_map = PB_TYPEMAP

    def create_schema(self):
        """
        Create python code to define pyarrow schema
        """
        template = Template(str_template)
        return template.render(
            protocol=self.protocol,
            entities=self.entities,
            tm=self.type_map,
            str=str
        )


def message_to_dict(message) -> Dict:
    """Convert protobuf message instance to dict

    args:
        -  message: protobuf message instance

    returns:
        - parameters and their values

    raises:
        :class:`.TypeError` if ``message`` is not a proto message
    """
    data = {}

    for desc, field in message.ListFields():
        if desc.type == desc.TYPE_MESSAGE:
            if desc.label == desc.LABEL_REPEATED:
                data[desc.name] = list(map(message_to_dict, field))
            else:
                data[desc.name] = message_to_dict(field)
        else:
            data[desc.name] = list(field) if desc.label == desc.LABEL_REPEATED else field

    return data
