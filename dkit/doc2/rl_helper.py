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
Various utilities to assist in rendering PDF documents with Reportlab.

Used mainly by rl_renderer module
"""

from pdfrw.toreportlab import makerl
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from pdfrw.buildxobj import pagexobj
from pdfrw import PdfReader
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    Paragraph, Flowable, TableStyle
)
from . import document as doc, fontsize_map


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
