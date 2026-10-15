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
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import (
    BaseDocTemplate, Frame, Image, ListFlowable, PageTemplate, Paragraph,
    SimpleDocTemplate, Spacer, Table, Preformatted, PageBreak, ListItem
)

from . import document as doc
from . import math_helper
from .rl_styles import DefaultStyler
from .rl_stylepack import StylePackStyler
from .rl_helper import is_pdf, TableHelper, PdfImage
from dkit.stylepack.model import StylePack

HEADING_COUNTER = 0


class _FlushMarginDocTemplate(SimpleDocTemplate):
    """SimpleDocTemplate whose body frame has no implicit padding.

    ReportLab's own Frame defaults to a 6pt padding on every side, which
    offsets body text from the page margin while header/footer text (drawn
    directly at the margin coordinate) is not. Zeroing the padding keeps
    body text aligned with the header/footer.
    """

    def build(self, flowables, onFirstPage=None, onLaterPages=None,
              canvasmaker=Canvas):
        self._calc()
        frame = Frame(
            self.leftMargin, self.bottomMargin, self.width, self.height,
            id="normal",
            leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0,
        )
        self.addPageTemplates([
            PageTemplate(
                id="First", frames=frame, onPage=onFirstPage,
                pagesize=self.pagesize,
            ),
            PageTemplate(
                id="Later", frames=frame, onPage=onLaterPages,
                pagesize=self.pagesize,
            ),
        ])
        BaseDocTemplate.build(self, flowables, canvasmaker=canvasmaker)


class RLRenderer:
    """Render document elements to PDF"""

    def __init__(
        self,
        document: doc.Document,
        allow_soft_breaks=False,
        styler=DefaultStyler,
        style_pack: StylePack | None = None,
    ):
        self.allow_soft_breaks = allow_soft_breaks  # allow breaks in a paragraph
        self.spacer_height = 0.0 * cm       # used by soft breaks
        self.paragraph_style = "BodyText"   # can change depending on type of block
        self.styler = (
            StylePackStyler(document, style_pack)
            if style_pack is not None and styler is DefaultStyler
            else styler(document)
        )
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

    @make.register(doc.Math)
    def make_math(self, element: doc.Math):
        if element.display:
            # its own block, vector, via the existing PdfImage flowable
            # also used for vector doc.Image content
            path = math_helper.render_math_file(element, "pdf")
            img = PdfImage(path)
            img.hAlign = element.align.upper()
            yield img
        else:
            # inline: a flowable breaks the surrounding paragraph onto
            # its own line, so this stays within the running text as a
            # Paragraph <img> tag instead -- which only supports raster
            # images, not the vector PdfImage path used for display math.
            # Width/height must be explicit: unlike PdfImage/Image, the
            # <img> tag ignores the PNG's own DPI metadata and falls
            # back to 72dpi, rendering far too large otherwise.
            path = math_helper.render_math_file(element, "png")
            width, height = math_helper.png_size_pt(path)
            # a numeric valign is a precise point offset from baseline to
            # the image's bottom edge (see imgVRange in reportlab's own
            # paragraph.py) -- needed because the image is cropped tight
            # to each formula's own ink, so the bottom-to-baseline
            # distance varies per formula (e.g. phi's descender loop)
            # and is not the fixed fraction of font size that keywords
            # like "bottom"/"middle" assume, which floated formulas
            # visibly above the surrounding text's baseline
            valign = -math_helper.inline_descent_pt(element)
            yield (
                f'<img src="{path}" width="{width}" height="{height}" '
                f'valign="{valign}"/>'
            )

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
        heading = Paragraph(
            self.make_text(element.content),
            self.styler[f"Heading{element.level}"]
        )
        # Keep section headings with the first flowable that follows them.
        # This prevents a heading from being stranded at the bottom of a page
        # while its paragraph, image, or table starts on the next page.
        heading.keepWithNext = 1
        yield heading

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph):
        # native markdown image syntax (![alt](src)) nests the Image inside
        # the paragraph's own content, unlike the {{ image(...) }} jinja
        # helper's Image, which is always its own top-level element --
        # make_text can only stringify inline text, so any embedded Image
        # is split out into its own flowable instead. Display math is
        # split out for the same reason (it renders to a PdfImage
        # flowable); inline math stays in the inline run since make_math
        # yields a plain <img> string for it, not a flowable.
        inlines = []
        for item in element.content:
            if isinstance(item, doc.Image) or (
                isinstance(item, doc.Math) and item.display
            ):
                if inlines:
                    yield Paragraph(
                        self.make_text(inlines), self.styler[self.paragraph_style]
                    )
                    inlines = []
                yield from self.make(item)
            else:
                inlines.append(item)
        if inlines:
            yield Paragraph(
                self.make_text(inlines), self.styler[self.paragraph_style]
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
            self.styler["Verbatim"]
        )

    @make.register(doc.Code)
    def make_code(self, element: doc.Code):
        font = self.styler["Verbatim"].fontName
        size = self.styler["Normal"].fontSize
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
        renderer = _FlushMarginDocTemplate(
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
