from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict
import typing
from ..data import json_utils as ju
from dataclass_wizard import JSONWizard


encoder = ju.JsonSerializer(
    ju.DateTimeCodec(),
    ju.DateCodec(),
    ju.Decimal2FloatCodec(),
    ju.PandasTimestampCodec(),
    ju.PandasNATCodec()
)


def as_json(obj):
    """convert to dict format"""
    return encoder.dumps(
        {
            "t": obj.__class__.__name__,
            "c": asdict(obj)
        }
    )


@dataclass
class Document:
    title: str
    sub_title: str
    author: str
    date: datetime = datetime.now()
    email: str = ""
    contact: str = ""


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
    height: float | None  = None


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

