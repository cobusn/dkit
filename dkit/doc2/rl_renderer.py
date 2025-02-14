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
Render dkit doc cannonical format to pdf using ReportLab
"""
import functools

from reportlab.lib.units import cm
from reportlab.platypus import (
    Image, ListFlowable, Paragraph, SimpleDocTemplate, Spacer,
    Table, Preformatted, PageBreak, ListItem
)

from . import document as doc
from .rl_styles import DefaultStyler
from .rl_helper import is_pdf, TableHelper, PdfImage

HEADING_COUNTER = 0


class RLRenderer:
    """Render document elements to PDF"""

    def __init__(self, document: doc.Document, allow_soft_breaks=False, styler=DefaultStyler):
        self.allow_soft_breaks = allow_soft_breaks  # allow breaks in a paragraph
        self.spacer_height = 0.0 * cm       # used by soft breaks
        self.paragraph_style = "BodyText"   # can change depending on type of block
        self.styler = styler(document)
        self.content = [PageBreak()]
        self.document = document

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

    def render(self, file_name: str):
        self.content.extend(self.iter_make_elements(self.document.elements))
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
