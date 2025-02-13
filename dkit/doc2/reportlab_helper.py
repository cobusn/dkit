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
import functools
from reportlab.platypus import (
    Image, ListFlowable, Paragraph, SimpleDocTemplate, Spacer, Flowable,
    Table, TableStyle, Preformatted, PageBreak, ListItem
)
from . import document as doc
from pdfrw import PdfReader
from pdfrw.buildxobj import pagexobj
from .reportlab_styles import RLStyler
from pdfrw.toreportlab import makerl
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.units import cm
from .renderer import render_doc_format
from jinja2 import Template
from .document import as_json
from reportlab.lib import colors
from . import fontsize_map
HEADING_COUNTER = 0


def _jsonise(doc_object) -> str:
    """Format output of function as json.

    Args:
        - fn: class with as_dict function

    Returns:
        - string
    """
    j = as_json(doc_object)
    return f"```jsoninclude\n{j}\n```\n"


class PdfImage(Flowable):
    """
    PdfImage wraps the first page from a PDF file as a Flowable which can
    ty
    Based on the vectorpdf extension in rst2pdf (http://code.google.com/p/rst2pdf/)
    """

    def __init__(self, filename_or_object, width=None, height=None, kind='direct'):
        # from reportlab.lib.units import inch
        # If using StringIO buffer, set pointer to begining
        if hasattr(filename_or_object, 'read'):
            filename_or_object.seek(0)
        page = PdfReader(filename_or_object, decompress=False).pages[0]
        self.xobj = pagexobj(page)
        self.imageWidth = width
        self.imageHeight = height
        x1, y1, x2, y2 = self.xobj.BBox

        self._w, self._h = x2 - x1, y2 - y1
        if not self.imageWidth:
            self.imageWidth = self._w
        if not self.imageHeight:
            self.imageHeight = self._h
        self.__ratio = float(self.imageWidth) / self.imageHeight
        if kind in ['direct', 'absolute'] or (width is None) or (height is None):
            self.drawWidth = width or self.imageWidth
            self.drawHeight = height or self.imageHeight
        elif kind in ['bound', 'proportional']:
            factor = min(float(width) / self._w, float(height) / self._h)
            self.drawWidth = self._w * factor
            self.drawHeight = self._h * factor

    def wrap(self, aW, aH):
        return self.drawWidth, self.drawHeight

    def drawOn(self, canv, x, y, _sW=0):
        if _sW > 0 and hasattr(self, 'hAlign'):
            a = self.hAlign
            if a in ('CENTER', 'CENTRE', TA_CENTER):
                x += 0.5 * _sW
            elif a in ('RIGHT', TA_RIGHT):
                x += _sW
            elif a not in ('LEFT', TA_LEFT):
                raise ValueError("Bad hAlign value " + str(a))

        xobj = self.xobj
        xobj_name = makerl(canv._doc, xobj)

        xscale = self.drawWidth / self._w
        yscale = self.drawHeight / self._h

        x -= xobj.BBox[0] * xscale
        y -= xobj.BBox[1] * yscale

        canv.saveState()
        canv.translate(x, y)
        canv.scale(xscale, yscale)
        canv.doForm(xobj_name)
        canv.restoreState()


class RLHeading(Paragraph):

    def draw(self):
        global HEADING_COUNTER
        key = f"ch{HEADING_COUNTER}"
        self.canv.bookmarkPage(key)
        self.canv.addOutlineEntry(self.getPlainText(), key, 0, None)
        HEADING_COUNTER += 1
        super().draw()


class BaseDocument(doc.Document):

    def __init__(self, title=None, sub_title=None, author=None, date=None,
                 email=None, contact=None):
        super().__init__(title, sub_title, author, date, email, contact)
        self.jinja_objects = {
            "image": self._jinja_include_image,
        }
        self.content = []

    def _jinja_include_image(self, source, title=None, width=None, height=None, align="center"):
        """
        include images using jinja templates
        """
        return _jsonise(
            doc.Image(
                source,
                title,
                align,
                width,
                height
            )
        )

    def add_element(self, element):
        self.content.extend([i for i in self.make(element)])

    def add_markdown(self, markdown, **objects):
        """parse and add markdown"""
        local = dict(self.jinja_objects)
        local.update(objects)
        rendered = Template(markdown).render(**local)
        elements = render_doc_format(rendered)
        self.content.extend(self.iter_make_elements(elements))


def is_pdf(name):
    """test if filename end with .pdf"""
    n = name.lower()
    if n.endswith(".pdf"):
        return True
    else:
        return False


class TableHelper(object):
    """Helper functions for formatting tables"""

    def __init__(self, table: doc.Table, local_style):
        self.table = table
        self.data = table.data
        self.lstyle = local_style
        self.columns = table.columns

    def formaters(self):
        return [column.formatter for column in self.columns]

    def col_alignments(self):
        aligns = []
        for i, column in enumerate(self.columns):
            align = column.align.upper()
            aligns.append(('ALIGN', (i, 0), (i, -1), align))
        return aligns

    def head_alignments(self):
        aligns = []
        for i, column in enumerate(self.columns):
            align = column.heading_align.upper()
            aligns.append(('ALIGN', (i, 0), (i, 0), align))
        return aligns

    def heading_color(self):
        textcolor = colors.HexColor(self.lstyle["table"]["heading_color"])
        background = colors.HexColor(self.lstyle["table"]["heading_background"])
        return [
            ("BACKGROUND", (0, 0), (-1, 0), background),
            ("TEXTCOLOR", (0, 0), (-1, 0), textcolor),
        ]

    def table_fonts(self):
        font_name = self.lstyle["table"]["font"]
        font_size = fontsize_map[self.lstyle["table"]["fontSize"]]
        font_color = colors.HexColor(self.lstyle["table"]["font_color"])
        return [
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ("FONT", (0, 0), (-1, -1), font_name),
            ("TEXTCOLOR", (0, 1), (-1, -1), font_color),
            ("FONTSIZE", (0, 0), (-1, -1), font_size),
        ]

    def table_style(self):
        """generate TableStyle instance"""
        styles = self.col_alignments() + self.head_alignments() + \
            self.heading_color() + self.table_fonts()
        return TableStyle(styles)

    def extract_data(self):
        """extract relevant fields from supplied data"""
        formatters = self.formaters()
        titles = [c.title for c in self.columns]
        tdata = [[f(row) for f in formatters] for row in self.data]
        return [titles] + tdata

    def widths(self):
        return [c.width * cm for c in self.columns]


class RLDocument(BaseDocument):

    def __init__(self, title=None, sub_title=None, author=None, date=None,
                 email=None, contact=None, allow_soft_breaks=True, styler=RLStyler):
        super().__init__(title, sub_title, author, date, email, contact)
        self.allow_soft_breaks = allow_soft_breaks  # allow breaks in a paragraph
        self.spacer_height = 0.0 * cm       # used by soft breaks
        self.paragraph_style = "BodyText"   # can change depending on type of block
        self.styler = styler(self)
        self.content = [PageBreak()]

    def iter_make_elements(self, elements):
        """build content from list of elements"""
        for element in elements:
            yield from self.make(element)

    def make_elements(self, elements):
        return list(self.iter_make_elements(elements))

    def make_text(self, elements):
        """make text elements"""
        return "".join(next(self.make(i)) for i in elements)

    @functools.singledispatchmethod
    def make(self, element):
        raise TypeError(f"Unsupported data type: {type(element)}")

    @make.register(doc.SoftBreak)
    def make_soft_break(self, element: doc.SoftBreak):
        if self.allow_soft_breaks:
            yield "<br/>"
        else:
            yield " "

    @make.register(doc.LineBreak)
    def make_line_break(self, element: doc.LineBreak):
        yield Spacer(width=0, height=self.spacer_height)

    @make.register(doc.Str)
    def make_str(self, element: doc.Str):
        yield element.text

    @make.register(doc.Link)
    def make_link(self, element: doc.Link):
        text = self.make_text(element.content)
        url = element.target
        yield f"<link href={url}><u>{text}</u></link>"

    @make.register(doc.Image)
    def make_image(self, element: doc.Image):
        width = element.width
        height = element.height
        data = element.source
        align = element.align.upper()

        _w = width * cm if width else None
        _h = height * cm if height else None
        if is_pdf(data):
            img = PdfImage(data, width=_w, height=_h)
        else:
            img = Image(data, width=_w, height=_h)
        img.hAlign = align
        yield img

    @make.register(doc.Emph)
    def make_emph(self, element):
        body = self.make_text(element.text)
        yield f"<i>{body}</i>"

    @make.register(doc.Bold)
    def make_bold(self, element):
        body = self.make_text(element.text)
        yield f"<b>{body}</b>"

    @make.register(doc.Heading)
    def make_heading(self, element: doc.Heading):
        yield Paragraph(
            self.make_text(element.content),
            self.styler[f"Heading{element.level}"]
        )

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph):
        yield Paragraph(
            self.make_text(element.content),
            self.styler[self.paragraph_style]   # style changed for block's
        )

    @make.register(doc.BlockQuote)
    def make_block_quote(self, element: doc.BlockQuote):
        # below, the style is changed to block quote and restored
        # once the block quote is rendered
        current_style = self.paragraph_style
        self.paragraph_style = "BlockQuote"
        yield from self.iter_make_elements(element.content)
        self.paragraph_style = current_style

    @make.register(doc.Block)
    def make_block(self, element: doc.Paragraph):
        # Blocks are used for example for list items
        yield Paragraph(
            self.make_text(element.content),
            self.styler["BodyText"]
        )

    @make.register(doc.List)
    def make_list(self, element: doc.List):
        if element.ordered:
            bt = "1"
            style = "OrderedList"
        else:
            bt = "bullet"
            style = "UnorderedList"

        yield ListFlowable(
            flowables=list(self.make_elements(element.content)),
            bulletType=bt,
            style=self.styler[style],
        )

    @make.register(doc.ListItem)
    def make_list_item(self, element: doc.ListItem):
        yield ListItem(
            self.make_elements(element.content),
            spaceBefore=0,
            spaceAfter=0
        )

    @make.register(doc.CodeBlock)
    def make_code_block(self, element: doc.CodeBlock):
        yield Preformatted(
            element.content,
            self.styler["Code"]
        )

    @make.register(doc.Code)
    def make_code(self, element: doc.Code):
        font = self.styler["Verbatim"].fontName
        size = self.styler["Verbatim"].fontSize
        color = self.styler["Verbatim"].textColor
        yield f"<font color='{color}' fontsize='{size}' face='{font}'>{element.content}</font>"

    @make.register(doc.HorizontalLine)
    def make_horizontal_line(self, elment: doc.HorizontalLine):
        raise NotImplementedError
        # yield Spacer(width=0, height=self.spacer_height)

    @make.register(doc.Table)
    def make_table(self, element: doc.Table):
        t = TableHelper(element, self.styler.local_style)
        table = Table(
            t.extract_data(),
            colWidths=t.widths(),
            repeatRows=1,
            hAlign=element.align.upper()
        )
        table.setStyle(t.table_style())
        yield table

    def render(self, file_name):
        renderer = SimpleDocTemplate(
            file_name,
            pagesize=self.styler.page_size,
            rightMargin=self.styler.right_margin,
            leftMargin=self.styler.left_margin,
            topMargin=self.styler.top_margin,
            bottomMargin=self.styler.bottom_margin
        )
        renderer.build(
            self.content,
            onFirstPage=self.styler.first_page,
            onLaterPages=self.styler.later_pages
        )


def as_jsoninclude(func):
    """Decorator that designate report output as json_include"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return _jsonise(func(*args, **kwargs))

    return wrapper
