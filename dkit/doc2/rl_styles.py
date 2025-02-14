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
from . import fontsize_map
import yaml
from reportlab.pdfbase.pdfmetrics import stringWidth
from importlib.resources import open_binary, open_text
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors, pagesizes
from reportlab.lib.units import cm
from reportlab.pdfgen.canvas import Canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from .rl_helper import PdfImage


class DefaultStyler(object):

    def __init__(self, document, local_style=None):
        if local_style:
            self.local_style = local_style
        else:
            self.local_style = self.load_local_style()
        self.doc = document
        self.style = getSampleStyleSheet()
        self.unit = cm
        self.register_fonts()
        self.update_styles()

    def __getitem__(self, key):
        return self.style[key]

    def load_local_style(self):
        """
        load default style update
        """
        with open_text("dkit.resources", "rl_stylesheet.yaml") as infile:
            return yaml.safe_load(infile)

    @property
    def title_date(self):
        fmt = self.local_style["page"]["title_date_format"]
        _date = self.doc.date if self.doc.date else datetime.now()
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
            "LETTER": pagesizes.LETTER
        }
        return size_map[self.local_style["page"]["size"].upper()]

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
        # print list of syles
        # print(list(self.style.byName.keys()))

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
            spaceBefore=5,
            spaceAfter=5,
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
            # self.__print_style(self.style.byName[style])

        # Update style / provided local stylesheet
        for style, updates in self.local_style["reportlab"]["styles"].items():
            this = self.style.byName[style]
            for k, v in updates.items():
                if "Color" in k:
                    setattr(this, k, colors.HexColor(v))
                elif k == "fontSize":
                    setattr(this, k, fontsize_map[v])
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

    def later_pages(self, canvas: Canvas, style_sheet):
        canvas.saveState()
        ty = self.page_height - self.top_margin
        tl = self.left_margin
        tr = self.page_width - self.right_margin
        by = self.bottom_margin

        # lines
        canvas.setStrokeColor(self.text_color)
        canvas.setFillColor(self.text_color)
        canvas.setLineWidth(0.1)

        canvas.line(tl, ty, tr, ty)   # top line
        canvas.line(tl, by, tr, by)   # bottom line

        # title
        canvas.setFont(self.title_font_name, 8)
        canvas.drawString(tl, ty + 12, self.doc.title)

        # subtitle
        canvas.setFont(self.author_font_name, 8)
        canvas.drawString(tl, ty + 2, self.doc.sub_title)

        # date
        canvas.setFont(self.author_font_name, 8)
        tw = stringWidth(self.title_date, self.author_font_name, 8)
        canvas.drawString(tr - tw, ty + 6, self.title_date)

        # author
        canvas.drawString(tl, by - 10, self.doc.author)
        tw = stringWidth(self.title_date, self.author_font_name, 8)

        # page number
        n = canvas.getPageNumber()
        page_num = str(f"Page: {n}")
        tw = stringWidth(page_num, self.author_font_name, 8)
        canvas.drawString(tr - tw, by - 10, page_num)

        canvas.restoreState()

    @property
    def contact_email(self):
        rv = self.doc.contact if self.doc.contact else ""
        if self.doc.email:
            if self.doc.contact:
                rv += f" / {self.doc.email}"
            else:
                rv = str(self.doc.email)
        return rv

    def first_page(self, canvas: Canvas, style_sheet):
        """default function for first pages"""
        canvas.saveState()
        canvas.setFont('Times-Bold', 16)

        # image
        # with open_binary("dkit.resources", "ddfrontpage.pdf") as infile:
        with open_binary("dkit.resources", "background.pdf") as infile:
            pdf = PdfImage(infile, self.page_width, self.page_height)
            pdf.drawOn(canvas, 0, 0)

        title_x = self.page_width / 15
        title_y = 2.2 * self.page_height / 3
        canvas.setFont(self.title_font_name, 22)
        # canvas.setFillColor(colors.white)
        canvas.setFillColor(colors.darkgray)
        canvas.drawString(title_x, title_y, self.doc.title)
        canvas.setFont(self.title_font_name, 16)
        canvas.drawString(title_x, title_y - 22, self.doc.sub_title)
        canvas.setFont(self.author_font_name, 14)
        canvas.drawString(title_x, title_y - 44, f"by: {self.doc.author}")
        canvas.drawString(title_x, title_y - 60, self.contact_email)
        canvas.setFont(self.author_font_name, 10)
        canvas.drawString(title_x, 160,  self.title_date)
        canvas.restoreState()
