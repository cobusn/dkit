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
Cannonical Document Structure that can be used to translate to
various other formats such as PDF, HTML, Word etc.
"""

from dataclasses import dataclass, asdict
import typing
from ..data import json_utils as ju
from dataclass_wizard import JSONWizard
from jinja2 import Template
from .md_to_doc import render_doc_format
import functools

encoder = ju.JsonSerializer(
    ju.DateTimeCodec(),
    ju.DateCodec(),
    ju.Decimal2FloatCodec(),
    ju.PandasTimestampCodec(),
    ju.PandasNATCodec()
)


def _jsonise(doc_object) -> str:
    """Format output of function as json.

    Args:
        - fn: class with as_dict function

    Returns:
        - string
    """
    j = as_json(doc_object)
    return f"```jsoninclude\n{j}\n```\n"


def as_json(obj):
    """convert to dict format"""
    return encoder.dumps(
        {
            "t": obj.__class__.__name__,
            "c": asdict(obj)
        }
    )


class Document:

    def __init__(self, title=None, sub_title=None, author=None, date=None,
                 email=None, contact=None):
        self.title = title
        self.sub_title = sub_title
        self.author = author
        self.date = date
        self.email = email
        self.contact = contact
        self.jinja_objects = {
            "image": self._jinja_include_image,
        }
        self.elements = []

    def _jinja_include_image(self, source, title=None, width=None, height=None, align="center"):
        """
        include images using jinja templates
        """
        return _jsonise(
            Image(
                source,
                title,
                align,
                width,
                height
            )
        )

    def add_element(self, element):
        self.elements.append(element)

    def add_markdown(self, markdown, **objects):
        """parse and add markdown"""
        local = dict(self.jinja_objects)
        local.update(objects)
        rendered = Template(markdown).render(**local)
        elements = render_doc_format(rendered)
        self.elements.extend(elements)
        # self.elements.extend(self.iter_make_elements(elements))


@dataclass
class Inline:
    text: str


@dataclass
class HorizontalLine:
    pass


@dataclass
class LineBreak:
    "Line break"


@dataclass
class SoftBreak:
    "Soft Break"


@dataclass
class Link:
    content: typing.List
    target: str


class _JsonIncludeMixin:

    def to_json(self) -> str:
        """format as jsoninclude block"""
        j = as_json(self)
        return f"```jsoninclude\n{j}\n```\n"


@dataclass
class Image(JSONWizard):
    """Image Object"""
    source: str
    title: str = None
    align: str = "center"
    width: float | None = None
    height: float | None = None


class Str(Inline):
    """strings"""


class Emph(Inline):
    """Emphasis"""


class Bold(Inline):
    """Bold"""


@dataclass
class Paragraph:
    content: typing.List


class Block(Paragraph):
    """Block Text"""


class BlockQuote(Paragraph):
    """Block Quote"""


@dataclass
class Code:
    content: str


@dataclass
class CodeBlock:
    """Code Block"""
    content: str
    language: str


@dataclass
class List:
    content: typing.List
    ordered: bool
    depth: int = None


@dataclass
class ListItem():
    content: List


@dataclass
class Heading:
    content: str
    level: int = 1


def from_json(json):
    """instantiate document object from JSON"""
    obj_map = {
        "Image": Image,
        "Table": Table,
    }
    obj_dict = encoder.loads(json)
    name = obj_dict["t"]
    content = obj_dict["c"]
    obj_type = obj_map[name]
    return obj_type.from_dict(content)


def _map_align(align):
    a_map = {"l": "left", "r": "right", "c": "center"}
    if align in a_map:
        align = a_map[align]
    return align


@dataclass
class _TableElement:

    def __post_init__(self):
        if not self.heading_align:
            self.heading_align = "center"
        _a = _map_align(self.align)
        assert _a in ["left", "right", "center"]
        self.align = _a


@dataclass
class Column(_TableElement):
    name: str
    title: str
    width: float = 2
    align: str = "left"
    heading_align: str = "center"
    dedup: bool = True
    format_: str = "{}"
    summary: bool = False

    def formatter(self, row):
        """format value

        used by TableHelper when generating table
        """
        data = row[self.name]
        return self.format_.format(data)


@dataclass
class SparkLine(_TableElement):
    spark_data: List
    master: str
    child: str
    value: str
    height = 0.3


@dataclass
class Table(JSONWizard):
    data: list[dict]
    columns: list[Column]
    align: str = "center"

    def __post_init__(self):
        _a = _map_align(self.align)
        assert _a in ["left", "right", "center"]
        self.align = _a

    @property
    def totals(self):
        """map of column totals"""
        totals = {}
        for k in self.style_map:
            if self.style_map[k].get("total", False):
                totals[k] = sum(i[k] for i in self.data)
        return totals

    def has_totals(self):
        if len([k for k in self.style_map.keys() if "total" in self.style_map[k]]) > 0:
            return True
        else:
            return False


def wrap_json(func):
    """Decorator that designate report output as json_include"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return _jsonise(func(*args, **kwargs))

    return wrapper
