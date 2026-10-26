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
Reportlab Style repository
"""
from datetime import datetime
from importlib.resources import open_binary, open_text
from pathlib import Path

import yaml
from reportlab.lib import colors, pagesizes
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import Paragraph
from pydantic import BaseModel
from dkit.utilities.jinja2 import render as render_template
from . import fontsize_map, document as doc
from .md_to_doc import render_doc_format
from .rl_helper import PdfImage


def _inline_markup(elements) -> str:
    """Convert canonical inline doc elements to ReportLab paragraph markup."""
    parts = []
    for item in elements:
        if isinstance(item, doc.Bold):
            parts.append(f"<b>{_inline_markup(item.text)}</b>")
        elif isinstance(item, doc.Emph):
            parts.append(f"<i>{_inline_markup(item.text)}</i>")
        elif isinstance(item, doc.Code):
            parts.append(f"<font face='Courier'>{item.content}</font>")
        elif isinstance(item, doc.Str):
            parts.append(item.text)
        elif isinstance(item, (doc.SoftBreak, doc.LineBreak)):
            # a header/footer zone's "text\ntext" is an explicit second
            # line, not running prose, so unlike body text a soft break
            # here always becomes a line break rather than a space.
            parts.append("<br/>")
        else:
            raise TypeError(
                f"unsupported header/footer markup element: {type(item)}"
            )
    return "".join(parts)


def markdown_to_rl_markup(text: str) -> str:
    """Render a short markdown string to ReportLab paragraph markup.

    Header/footer zones only need inline formatting (bold/italic/code)
    and explicit line breaks, not the full block grammar, so each
    top-level block is joined with ``<br/>``.
    """
    blocks = render_doc_format(text)
    parts = [
        _inline_markup(block.content)
        for block in blocks
        if isinstance(block, doc.Paragraph)
    ]
    return "<br/>".join(parts)


class DefaultStyler(object):
    """default rl_renderer styler

    Used by rl_render to generate pdf documents
    this class can be inherited to implement new styles
    for different document standards
    """

    def __init__(
        self,
        document: doc.Document,
        local_style: dict = None,
        background_path: str | Path | None = None,
        template_context: dict | None = None,
    ):
        if local_style:
            self.local_style = local_style
        else:
            self.local_style = self.load_local_style(
                "dkit.resources", "rl_stylesheet.yaml"
            )
        self.doc = document
        self.template_context = template_context or {}
        self.background_path = (
            Path(background_path) if background_path is not None else None
        )
        self.style = getSampleStyleSheet()
        self.unit = cm
        self.register_fonts()
        self.update_styles()

    def __getitem__(self, key):
        return self.style[key]

    def load_local_style(self, location, filename):
        """
        load default style update

        arguments:
            - location: module name
            - filename: yaml file name
        """
        with open_text(location, filename) as infile:
            return yaml.safe_load(infile)

    @property
    def title_date(self):
        fmt = self.local_style["page"]["title_date_format"]
        _date = self.doc._title_date if self.doc._title_date else self.doc._date
        return fmt.format(_date)

    @property
    def left_margin(self):
        return self.local_style["page"]["left"] / 10 * self.unit

    @property
    def right_margin(self):
        return self.local_style["page"]["right"] / 10 * self.unit

    @property
    def top_margin(self):
        return self.local_style["page"]["top"] / 10 * self.unit

    @property
    def bottom_margin(self):
        return self.local_style["page"]["bottom"] / 10 * self.unit

    @property
    def page_size(self):
        size_map = {
            "A4": pagesizes.A4,
            "A5": pagesizes.A5,
            "LETTER": pagesizes.LETTER,
            "LEGAL": pagesizes.LEGAL,
        }
        size = size_map[self.local_style["page"]["size"].upper()]
        if self.local_style["page"].get("orientation", "portrait").lower() == "landscape":
            return pagesizes.landscape(size)
        return size

    @property
    def page_width(self):
        return self.page_size[0]

    @property
    def page_height(self):
        return self.page_size[1]

    @property
    def title_font_name(self):
        return self["Heading1"].fontName

    @property
    def author_font_name(self):
        return self["Heading3"].fontName

    @property
    def text_color(self):
        """primary text color"""
        return self.style["Normal"].textColor

    def register_fonts(self):
        """load additional fonts"""
        def register_font(font_name, file_name):
            """load font from resources and register"""
            with open_binary("dkit.resources", file_name) as infile:
                pdfmetrics.registerFont(TTFont(font_name, infile))

        register_font("SourceSansPro", "SourceSansPro-Regular.ttf")
        register_font("SourceSansPro-Bold", "SourceSansPro-Bold.ttf")
        register_font("SourceSansPro-Italic", "SourceSansPro-Italic.ttf")
        register_font("SourceSansPro-Ital", "SourceSansPro-Italic.ttf")
        register_font("SourceSansPro-BoldItalic", "SourceSansPro-BoldItalic.ttf")
        pdfmetrics.registerFontFamily(
            "SourceSansPro", "SourceSansPro", "SourceSansPro-Bold",
            "SourceSansPro-Ital", "SourceSansPro-BoldItalic"
        )

    def __print_style(self, style):
        """helper to print style info"""
        for k, v in sorted(style.__dict__.items(), key=lambda x: x[0]):
            print(k, v)

    def update_styles(self):

        # Create Verbatim style
        self.style.byName['Verbatim'] = ParagraphStyle(
            'Verbatim',
            parent=self.style['Normal'],
            firstLineIndent=20,
            leftIndent=20,
            fontName="Times-Roman",
            fontSize=8,
            leading=8,
            spaceAfter=5,
            spaceBefore=5,
        )
        # self.__print_style(self.style.byName["Verbatim"])

        # BlockQuote
        self.style.byName['BlockQuote'] = ParagraphStyle(
            'BlockQuote',
            parent=self.style['Normal'],
            firstLineIndent=10,
            leftIndent=10,
        )

        # Update Code style
        code = self.style.byName["Code"]
        code.spaceBefore = 5
        code.spaceAfter = 5
        code.leftIndent = 10

        # Bodytext
        bt = self.style.byName["BodyText"]
        bt.spaceBefore = 5
        bt.spaceAfter = 5

        # Update List styles
        for style in ["OrderedList", "UnorderedList"]:
            ol = self.style.byName[style]
            ol.spaceBefore = 0
            ol.spaceAfter = 0
            ol.leftIndent = 10
            ol.bulletFontSize = self.style["BodyText"].fontSize
            ol.bulletColor = self.style["BodyText"].textColor

        # Update style / provided local stylesheet
        for style, updates in self.local_style["reportlab"]["styles"].items():
            this = self.style.byName[style]
            for k, v in updates.items():
                if "Color" in k:
                    setattr(this, k, colors.HexColor(v))
                elif k == "fontSize":
                    setattr(
                        this, k,
                        fontsize_map[v] if isinstance(v, str) else v
                    )
                else:
                    setattr(this, k,  v)

        # Update List styles
        for style in ["OrderedList", "UnorderedList"]:
            ol = self.style.byName[style]
            ol.spaceBefore = 0
            ol.spaceAfter = 0
            ol.leftIndent = 10
            ol.bulletFontSize = self.style["BodyText"].fontSize
            ol.bulletColor = self.style["BodyText"].textColor
            # self.__print_style(self.style.byName[style])

    def first_page(self, canvas: Canvas, style_sheet):
        """default function for first pages"""
        class FirstPageConf(BaseModel):
            package: str
            image: str
            text_color: str
            title_color: str
            title_xy: tuple[int, int]
            title_font_size: int
            subtitle_xy: tuple[int, int]
            subtitle_font_size: int
            author_xy: tuple[int, int]
            author_font_size: int
            contact_xy: tuple[int, int]
            contact_font_size: int
            date_font_size: int
            date_xy: tuple[int, int]

        canvas.saveState()
        # canvas.setFont('Times-Bold', 16)
        conf = FirstPageConf(**self.local_style["reportlab"]["front_page"])

        # image
        if self.background_path is not None:
            pdf = PdfImage(
                str(self.background_path), self.page_width, self.page_height
            )
            pdf.drawOn(canvas, 0, 0)
        else:
            with open_binary(conf.package, conf.image) as infile:
                pdf = PdfImage(infile, self.page_width, self.page_height)
                pdf.drawOn(canvas, 0, 0)

        # color
        canvas.setFillColor(conf.title_color)

        # title
        x, y = conf.title_xy
        canvas.setFont(self.title_font_name, conf.title_font_size)
        canvas.drawString(x, y, self.doc.title or "")

        # main color
        canvas.setFillColor(conf.text_color)

        # subtitle
        x, y = conf.subtitle_xy
        canvas.setFont(self.title_font_name, conf.subtitle_font_size)
        canvas.drawString(x, y, self.doc.sub_title or "")

        # author
        x, y = conf.author_xy
        canvas.setFont(self.author_font_name, conf.author_font_size)
        canvas.drawString(x, y, self.doc.author or "")

        # email
        x, y = conf.contact_xy
        canvas.setFont(self.author_font_name, conf.contact_font_size)
        canvas.drawString(x, y, self.doc.contact or "")

        # date
        x, y = conf.date_xy
        canvas.setFont(self.author_font_name, conf.date_font_size)
        canvas.drawString(x, y,  self.title_date)
        canvas.restoreState()

    def later_pages(self, canvas: Canvas, style_sheet):
        class LaterPage(BaseModel):
            top_line: bool
            bottom_line: bool
            header_left: str | None = None
            header_right: str | None = None
            footer_left: str | None = None
            footer_right: str | None = None

        conf = LaterPage(**self.local_style["reportlab"]["later_pages"])
        canvas.saveState()
        ty = self.page_height - self.top_margin
        tl = self.left_margin
        tr = self.page_width - self.right_margin
        by = self.bottom_margin

        # lines
        canvas.setStrokeColor(self.text_color)
        canvas.setFillColor(self.text_color)
        canvas.setLineWidth(0.1)
        if conf.top_line:
            canvas.line(tl, ty, tr, ty)
        if conf.bottom_line:
            canvas.line(tl, by, tr, by)   # bottom line

        context = {
            "title": self.doc.title or "",
            "sub_title": self.doc.sub_title or "",
            "author": self.doc.author or "",
            "contact": self.doc.contact or "",
            "version": self.doc.version or "",
            "date": self.title_date,
            "page": canvas.getPageNumber(),
            "year": datetime.now().year,
            **self.template_context,
        }
        gap = 4
        zone_width = (tr - tl) / 2 - gap
        left_style = ParagraphStyle(
            "HeaderFooterLeft",
            fontName=self.author_font_name,
            fontSize=8,
            leading=9,
            textColor=self.text_color,
            alignment=TA_LEFT,
        )
        right_style = ParagraphStyle(
            "HeaderFooterRight", parent=left_style, alignment=TA_RIGHT,
        )

        def draw_zone(template, x, anchor_y, grows_down, para_style):
            if not template:
                return
            markup = markdown_to_rl_markup(render_template(template, **context))
            if not markup:
                return
            paragraph = Paragraph(markup, para_style)
            _, height = paragraph.wrapOn(canvas, zone_width, self.page_height)
            y = anchor_y - height if grows_down else anchor_y
            paragraph.drawOn(canvas, x, y)

        # headers grow downward from the top margin; footers grow upward
        # from the bottom margin, so a second line extends away from the
        # body text in both cases rather than into it. The anchors sit
        # inside the page's own margin whitespace (above ty / below by),
        # not at the frame edge itself, so a two-line zone never overlaps
        # the body frame.
        header_anchor = ty + 20
        footer_anchor = by - 20
        draw_zone(conf.header_left, tl, header_anchor, True, left_style)
        draw_zone(
            conf.header_right, tr - zone_width, header_anchor, True,
            right_style
        )
        draw_zone(conf.footer_left, tl, footer_anchor, False, left_style)
        draw_zone(
            conf.footer_right, tr - zone_width, footer_anchor, False,
            right_style
        )

        canvas.restoreState()
