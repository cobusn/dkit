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
Extension for pyarrow

=========== =============== =================================================
July 2019   Cobus Nel       Initial version
=========== =============== =================================================
"""


from ..writer import ClosedWriter
from ...data.iteration import chunker
from ..model import Entity
from itertools import islice, chain
import pyarrow as pa
from jinja2 import Template

__all__ = ["OSFileWriter"]

str_template = """
import pyarrow as pa

{% for entity, typemap in entities.items() %}

# {{ entity }}
schema_{{ entity }} = pa.schema(
    [
        {% for field, props in typemap.schema.items() -%}
          {% if "nullable" in props -%}
            {% set nullable = ", " + str(props["nullable"]) -%}
          {% else -%}
            {% set nullable = "" -%}
        {% endif -%}
        pa.field("{{ field }}", pa.{{ tm[props["type"]] }}(){{ nullable }}),
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

# convert cannonical to arrow
ARROW_TYPEMAP = {
    "float":   pa.float32(),
    "double":  pa.float64(),
    "integer": pa.int32(),
    "int8":    pa.int16(),    # int8 not available
    "int16":   pa.int16(),
    "int32":   pa.int32(),
    "int64":   pa.int64(),
    "string":  pa.string(),
    "boolean": pa.bool_(),
    "binary":  pa.binary(),
    # "datetime":  pa.time32("s"),
    "datetime":  pa.timestamp("s"),
    "date": pa.date32(),
    "decimal": pa.decimal128(11, 3),   # this probably need optimisation
}


def make_arrow_schema(cannonical_schema: Entity):
    """create an Arrow schema from cannonical Entity"""
    fields = []
    validator = cannonical_schema.as_entity_validator()
    for name, definition in validator.schema.items():
        fields.append(
            pa.field(
                name,
                ARROW_TYPEMAP[definition["type"]]
            )
        )
    return pa.schema(fields)


class ArrowSchemaGenerator(object):
    """
    Create .py file that define pyarrow schema fromm
    cannonical entity schema (s).
    """

    def __init__(self, **entities):
        self.__entities = entities
        self.type_map = {
            k: self.str_name(v)
            for k, v in
            ARROW_TYPEMAP.items()
        }

    @staticmethod
    def str_name(obj):
        """appropriate string name for pyarrow object"""
        sn = str(obj)
        if sn == "float":
            sn = f"{sn}{obj.bit_width}"
        return sn

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


def infer_arrow_schema(iterable, n=50):
    """
    infer schema from iterable

    returns:
        * arrow schema
        * a reconstructed iterable
    """
    i = iter(iterable)
    buffer = list(islice(i, n))
    schema = make_arrow_schema(
        Entity.from_iterable(buffer, p=1.0, k=n)
    )
    return schema, chain(buffer, i)


class OSFileWriter(ClosedWriter):
    """
    pyarrow OSWriter abstraction

    :param path: file path
    :param mode: file mode
    """
    def __init__(self, path, mode="rb"):
        self.path = path
        self.mode = mode
        self.pa = pa

    def open(self):
        return self.pa.OSFile(self.path, self.mode)


def build_table(data, schema=None, micro_batch_size=100_000) -> pa.Table:
    """build pyarrow table"""

    def iter_batch():
        for chunk in chunker(data, size=micro_batch_size):
            yield pa.RecordBatch.from_pylist(
                list(chunk),
                schema=schema
            )

    return pa.Table.from_batches(
        iter_batch()
    )
