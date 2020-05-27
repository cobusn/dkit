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
Create Document Artifacts

=========== =============== =================================================
2019        Cobus Nel       Created
=========== =============== =================================================
"""
from .. import __version__
from ..utilities.mixins import SerDeMixin
from ..plot import ggrammar
import collections
import textwrap
from abc import ABC, abstractmethod

__report_version__ = "0.2"


def _map_align(align):
    a_map = {"l": "left", "r": "right", "c": "center"}
    if align in a_map:
        align = a_map[align]
    return align


class DocumentContainer(collections.MutableMapping):

    def __init__(self, *args, **kwargs):
        self.elements = []
        self.store = dict()
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key


class Document(DocumentContainer):

    def __init__(self, title=None, sub_title=None, author=None):
        super().__init__()
        self.title = title
        self.sub_title = sub_title
        self.author = author

    def __add__(self, other):
        other.modify(self)

    def __iadd__(self, other):
        other.modify(self)
        return self

    def as_dict(self):
        return {
            "library_version": __version__,
            "report_version": __report_version__,
            "title": self.title,
            "sub_title": self.sub_title,
            "author": self.author,
            "data": self.store,
            "elements": [i.as_dict() for i in self.elements]
        }


class Element(SerDeMixin):

    def __init__(self, *kwds, **kwargs):
        self._parent_ = None


class Modifier(Element):

    def __init__(self, data, *kwds, **kwargs):
        self.data = data
        super().__init__(*kwds, **kwargs)


class ListModifier(Element, collections.MutableSequence):

    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data
        # if isinstance(data, [list, tuple, set]):
        #    self.data = list()
        #    self.data.extend(data)

    def __len__(self):
        return len(self.inner_list)

    def __getitem__(self, index):
        return self.inner_list[index]

    def __delitem__(self, index):
        del self.inner_list[index]

    def insert(self, index, value):
        self.inner_list.insert(index, value)

    def __setitem__(self, index, value):
        self.inner_list[index] = value


class Title(Modifier):

    def modify(self, other: Document):
        other.title = self.data


class SubTitle(Modifier):

    def modify(self, other: Document):
        other.subtitle = self.data


class Author(Modifier):

    def modify(self, other: Document):
        other.author = self.data


class _AddElement(Modifier):

    def __init__(self, data, *kwds, **kwargs):
        super().__init__(data, *kwds, **kwargs)

    def modify(self, other: Document):
        other.elements.append(self)
        self._parent_ = other


class _ListElement(ListModifier):

    def modify(self, other: Document):
        other.elements.append(self)
        self._parent_ = other


class Paragraph(_ListElement):
    pass


class FootnoteItem(_ListElement):

    def __init__(self, text, key, *kwds, **kwargs):
        super().__init__(text, *kwds, **kwargs)
        self.key = key


class FootnoteRef(_AddElement):

    def __init__(self, index, key, *kwds, **kwargs):
        super().__init__(index, *kwds, **kwargs)
        self.key = key


class Footnotes(_AddElement):
    pass


class Text(_AddElement):
    pass


class Heading(_ListElement):

    def __init__(self, elements, level, *kwds, **kwargs):
        super().__init__(elements, *kwds, **kwargs)
        self.level = level


class AutoLink(_AddElement):

    def __init__(self, link, is_email=False, *kwds, **kwargs):
        super().__init__(link, *kwds, **kwargs)
        self.is_email = is_email


class Link(_AddElement):

    def __init__(self, text, url, title, *kwds, **kwargs):
        super().__init__(text, *kwds, **kwargs)
        self.url = url
        self.title = title


class Bold(_AddElement):
    """bold text"""
    pass


class MD(_AddElement):

    def as_dict(self):
        return {
            "~>": "md",
            "data": textwrap.dedent(self.data)
        }


class BlockQuote(_AddElement):
    pass


class Image(_AddElement):

    def __init__(self, source,  title, text, align="center", *kwds, **kwargs):
        super().__init__(source, *kwds, **kwargs)
        self.title = title
        self.align = _map_align(align)
        self.text = text


class Listing(_AddElement):

    def __init__(self, source, language, *kwds, **kwargs):
        super().__init__(source, *kwds, **kwargs)
        self.language = language


class List(_AddElement):

    class Entry(_AddElement):
        pass

    def __init__(self, ordered, *kwds, **kwargs):
        super().__init__([], *kwds, **kwargs)
        self.ordered = ordered

    def add_entry(self, element):
        self.data.append(element)


class LineBreak(_AddElement):
    pass


class Table(_AddElement):

    class __TableElement(Element):

        def __init__(self,  title=None, width=2, align="left", heading_align="center"):
            self.title = str(title)
            self.width = width
            self.align = _map_align(align)
            self.heading_align = _map_align(heading_align)

        def modify(self, other):
            other.fields.append(self)

        def format(self, row):
            raise NotImplementedError

    class SparkLine(__TableElement):

        def __init__(self, spark_data, master_field, child_field, value_field, title=None, width=2,
                     heading_align="center"):
            super().__init__(title, width, align="center", heading_align=heading_align)
            self.spark_data = spark_data
            self.master = master_field
            self.child = child_field
            self.value = value_field
            self.height = 0.3

    class Field(__TableElement):
        """Field

        Args:
            * name
            * title
            * width
            * align
            * format_
            * summary
            * dedup
            * heading_align
            * symbol

        """
        def __init__(self,  name, title=None, width=2, align="left", format_="{}",
                     summary=None, dedup=True, heading_align="center", symbol=None):
            super().__init__(title, width, align, heading_align)
            self.name = name
            self.dedup = dedup
            self.format_ = format_
            self.summary = summary

    def __init__(self, data, fields=None, align="center", font_size=None,
                 *kwds, **kwargs):
        self.fields = fields if fields else []
        _a = _map_align(align)
        assert _a in ["left", "right", "center"]
        self.align = _a
        self.font_size = font_size
        super().__init__(list(data), *kwds, **kwargs)

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


class Figure(ggrammar.PlotBase, _AddElement, SerDeMixin):

    AnchoredText = ggrammar.AnchoredText
    GeomArea = ggrammar.GeomArea
    GeomBar = ggrammar.GeomBar
    GeomCumulative = ggrammar.GeomCumulative
    GeomDelta = ggrammar.GeomDelta
    GeomHistogram = ggrammar.GeomHistogram
    GeomImpulse = ggrammar.GeomImpulse
    GeomLine = ggrammar.GeomLine
    GeomScatter = ggrammar.GeomScatter
    GeomTreeMap = ggrammar.GeomTreeMap
    Title = ggrammar.Title
    XAxis = ggrammar.XAxis
    YAxis = ggrammar.YAxis
    HLine = ggrammar.HLine
    VLine = ggrammar.VLine

    def __init__(self, data, filename: str = None, *args, **kwargs):
        super().__init__(data, *args, **kwargs)
        self.filename = filename


class Inline(_AddElement):
    """inline verbatim text"""
    pass


class Emphasis(_AddElement):
    """inline verbatim text"""
    pass


class Verbatim(_AddElement):
    """All text rendered verbatim"""

    def __init__(self, data, language=None, *kwds, **kwargs):
        self.language = language
        super().__init__(data, *kwds, **kwargs)


class Latex(_AddElement):
    """Latex code"""
    pass


class HRule(_AddElement):
    """horisontal rule"""
    def __init__(self, *kwds, **kwargs):
        super().__init__(0, *kwds, **kwargs)


class AbstractRenderer(ABC):

    def __init__(self, data):
        self.data = data
        self.callbacks = {
            "bold": self.make_bold,
            "blockquote": self.make_block_quote,
            "emphasis": self.make_emphasis,
            "entry": self.make_entry,
            "figure": self.make_figure,
            "heading": self.make_heading,
            "image": self.make_image,
            "inline": self.make_inline,
            "latex": self.make_latex,
            "link": self.make_link,
            "list": self.make_list,
            "linebreak": self.make_line_break,
            "listing": self.make_listing,
            "md": self.make_markdown,
            "paragraph": self.make_paragraph,
            "table":  self.make_table,
            "text": self.make_text,
            "verbatim": self.make_verbatim,
        }

    @abstractmethod
    def make_bold(self, element):
        pass

    @abstractmethod
    def make_emphasis(self, element):
        pass

    @abstractmethod
    def make_figure(self, data):
        pass

    @abstractmethod
    def make_heading(self, element):
        pass

    @abstractmethod
    def make_image(self, element):
        pass

    @abstractmethod
    def make_inline(self, element):
        pass

    @abstractmethod
    def make_latex(self, data):
        pass

    @abstractmethod
    def make_line_break(self, element):
        pass

    @abstractmethod
    def make_list(self, element):
        pass

    @abstractmethod
    def make_link(self, element):
        pass

    @abstractmethod
    def make_listing(self, element):
        pass

    @abstractmethod
    def make_markdown(self, element):
        pass

    @abstractmethod
    def make_paragraph(self, element):
        pass

    @abstractmethod
    def make_text(self, element):
        pass

    @abstractmethod
    def make_table(self, data):
        pass

    @abstractmethod
    def make_verbatim(self, data):
        pass


__all__ = [
    AbstractRenderer,
    Author,
    AutoLink,
    BlockQuote,
    Bold,
    Document,
    DocumentContainer,
    Element,
    Emphasis,
    Figure,
    FootnoteItem,
    FootnoteRef,
    Footnotes,
    HRule,
    HRule,
    Heading,
    Image,
    Inline,
    Latex,
    LineBreak,
    Link,
    List,
    MD,
    Paragraph,
    SubTitle,
    Table,
    Title,
    Verbatim,
]
