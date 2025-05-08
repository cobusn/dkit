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
import functools
import os
import tempfile
import typing
from dataclasses import dataclass, asdict
from datetime import datetime

from dataclass_wizard import JSONWizard
from jinja2 import Template

from ..data import json_utils as ju
from .md_to_doc import render_doc_format


encoder = ju.JsonSerializer(
    ju.DateTimeCodec(),
    ju.DateCodec(),
    ju.Decimal2FloatCodec(),
    # uncomment below if pandas needed
    # ju.PandasTimestampCodec(),
    # ju.PandasNATCodec()
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
    """
    Initialize a document

    Args:
        - title: document main title
        - sub_title: document subtitle
        - author: document author
        - date: defaults to today's date. Else provide a string
        - contact: contact details for title page
    """

    def __init__(self, title: str = None, sub_title: str = None,
                 author: str = None, title_date: str = None, contact: str = None):
        self.title = title
        self.sub_title = sub_title
        self.author = author
        self._title_date = title_date
        self._date = datetime.now()
        self.contact = contact
        self.jinja_objects = {
            "image": self._jinja_include_image,
            "page_break": self._jinja_include_page_break,
        }
        self.elements = []

    def _jinja_include_page_break(self, threshold=None):
        """
        include images using jinja templates
        """
        return _jsonise(
            PageBreak(threshold)
        )

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

    def add_template_files(self, files: list[str], **objects: object):
        """add markdown templates from a list of files"""
        for fname in files:
            with open(fname, "rt") as infile:
                text = infile.read()
                self.add_template(text, **objects)

    def add_template(self, template, **objects):
        """parse and add markdown"""
        local = dict(self.jinja_objects)
        local.update(objects)
        rendered = Template(template).render(**local)
        elements = render_doc_format(rendered)
        self.elements.extend(elements)


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
class PageBreak(JSONWizard):
    "Line break"
    threshold: int | None = None


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
        "PageBreak": PageBreak,
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
    """
    Table Column

    args:
        - name: field name
        - title: display title
        - width: in cm
        - align: left, right, center
        - heading_align: left, right, center"
        - dedup: hide cells the same as the one above (not implemented)
        - format_: string format
        - summary: True if adding summation to last row (not implemented)
    """
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


def wrap_matplotlib(filename=None, align="center", width=None, height=None, kind=".png"):
    """wrap matplotlib object.

    make sure the function return a pyplot object

    NOTE: IF no filename is specified, a temporary pdf file will be created
    in the /tmp folder
    """
    assert kind in [".pdf", ".jpg", ".png"]

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            plt = func(*args, **kwargs)
            if not filename:
                fd, name = tempfile.mkstemp(suffix=kind)
                os.close(fd)
            else:
                name = filename
            plt.savefig(name)
            rv = _jsonise(
                Image(name, align=align, width=width, height=height)
            )
            return rv
        return wrapper
    return decorator


def wrap_imagebytes(filename=None, align="center", width=None, height=None):
    """wrap image from BytesIO object

    make sure the function return a BytesIO image

    NOTE: IF no filename is specified, a temporary png file will be created
    in the /tmp folder
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            png = func(*args, **kwargs)
            if png is None or png.getvalue() is None:
                return ''
            if not filename:
                fd, name = tempfile.mkstemp(suffix=".png")
                os.close(fd)
            else:
                name = filename
            with open(name, 'wb') as f:
                f.write(png.getvalue())
            rv = _jsonise(
                Image(name, align=align, width=width, height=height)
            )
            return rv
        return wrapper
    return decorator


def wrap_json(func):
    """Decorator that designate report output as json_include"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return _jsonise(func(*args, **kwargs))

    return wrapper
