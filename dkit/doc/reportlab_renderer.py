import warnings
from abc import ABC, abstractmethod
from importlib.resources import open_binary, open_text
from dkit.utilities.file_helper import yaml_load

import mistune
from pdfrw import PdfReader
from reportlab.pdfbase.pdfmetrics import stringWidth
from pdfrw.buildxobj import pagexobj
from pdfrw.toreportlab import makerl
from reportlab.lib import colors, pagesizes
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfgen.canvas import Canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image, ListFlowable, Paragraph, SimpleDocTemplate, Spacer, Flowable,
    Table, TableStyle, Preformatted, PageBreak
)
from . import fontsize_map
from ..plot import matplotlib as mpl
from ..utilities.introspection import is_list
from .document import AbstractRenderer, Document, DictDocument
from .json_renderer import JSONRenderer
from .. import messages


# from ..utilities.file_helper import temp_filename


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
        self.__ratio = float(self.imageWidth)/self.imageHeight
        if kind in ['direct', 'absolute'] or (width is None) or (height is None):
            self.drawWidth = width or self.imageWidth
            self.drawHeight = height or self.imageHeight
        elif kind in ['bound', 'proportional']:
            factor = min(float(width)/self._w, float(height)/self._h)
            self.drawWidth = self._w*factor
            self.drawHeight = self._h*factor

    def wrap(self, aW, aH):
        return self.drawWidth, self.drawHeight

    def drawOn(self, canv, x, y, _sW=0):
        if _sW > 0 and hasattr(self, 'hAlign'):
            a = self.hAlign
            if a in ('CENTER', 'CENTRE', TA_CENTER):
                x += 0.5*_sW
            elif a in ('RIGHT', TA_RIGHT):
                x += _sW
            elif a not in ('LEFT', TA_LEFT):
                raise ValueError("Bad hAlign value " + str(a))

        xobj = self.xobj
        xobj_name = makerl(canv._doc, xobj)

        xscale = self.drawWidth/self._w
        yscale = self.drawHeight/self._h

        x -= xobj.BBox[0] * xscale
        y -= xobj.BBox[1] * yscale

        canv.saveState()
        canv.translate(x, y)
        canv.scale(xscale, yscale)
        canv.doForm(xobj_name)
        canv.restoreState()


class Formatter(ABC):

    def __init__(self, field):
        self.field = field

    @abstractmethod
    def __call__(self, row):
        """to be implemented"""


class FieldFormatter(Formatter):

    def __call__(self, row):
        data = row[self.field["name"]]
        fmt = self.field["format_"]
        return fmt.format(data)


class TableHelper(object):
    """Helper functions for formatting tables"""

    def __init__(self, table, local_style):
        self.table = table
        self.data = table["data"]
        self.lstyle = local_style
        self.fields = table["fields"]
        self.format_map = {"field": FieldFormatter, }

    def formaters(self):
        return [self.format_map[f["~>"]](f) for f in self.fields]

    def col_alignments(self):
        aligns = []
        for i, field in enumerate(self.fields):
            align = field["align"].upper()
            aligns.append(('ALIGN', (i, 0), (i, -1), align))
        return aligns

    def head_alignments(self):
        aligns = []
        for i, field in enumerate(self.fields):
            align = field["heading_align"].upper()
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
        titles = [f["title"] for f in self.fields]
        tdata = [[f(row) for f in formatters] for row in self.data]
        return [titles] + tdata

    def widths(self):
        return [f["width"] * cm for f in self.fields]


class RLStyler(object):

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
            return yaml_load(infile)

    @property
    def title_date(self):
        fmt = self.local_style["page"]["title_date_format"]
        return fmt.format(self.doc.date)

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
            spaceAfter=10,
        )

        # Print all attributes of Verbatim
        # self.__print_style(self.style.byName['BodyText'])

        # BlockQuote
        self.style.byName['BlockQuote'] = ParagraphStyle(
            'BlockQuote',
            parent=self.style['Normal'],
            firstLineIndent=10,
            leftIndent=10,
            spaceBefore=10,
            spaceAfter=10,
        )

        # Update Code style
        code = self.style.byName["Code"]
        code.spaceBefore = 10
        code.spaceAfter = 10

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

        # text

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
        # with open_binary("dkit.resources", "background.pdf") as infile:
        with open_binary("dkit.resources", "ddfrontpage.pdf") as infile:
            pdf = PdfImage(infile, self.page_width, self.page_height)
            pdf.drawOn(canvas, 0, 0)

        title_x = self.page_width/15
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


class ReportlabDocRenderer(AbstractRenderer):
    """
    Render a cannonical json like formatted document
    to pdf using the Reportlab library.

    Although the Latex version will produce better
    layouts, this version is useful for generating
    pdf documents without littering the filesystem
    with tex files.
    """
    text_nodes = ["text", "bold", "emphasis", "inline", "link"]

    def __init__(self, data, styler):
        super().__init__(data)
        self.plot_backend = mpl.MPLBackend
        self.styler = styler
        self.in_paragraph = False

    def make_bold(self, element):
        """format bold"""
        content = self._make(element["data"])
        return f"<b>{content}</b>"

    def make_emphasis(self, element):
        """format italic"""
        content = self._make(element["data"])
        return f"<i>{content}</i>"

    def make_figure(self, data):
        """format a plot"""
        # filename = str(temp_filename(suffix="svg"))
        be = self.plot_backend(
            terminal="pdf",
            style_sheet=self.styler.local_style
        )
        pdf_data = be.render_mem(data)
        flowable = PdfImage(pdf_data)

        # This should be an option in the data provided..
        flowable.hAlign = "CENTER"
        return flowable

    def make_heading(self, element):
        """format heading"""
        level = element["level"]
        heading = Paragraph(
            self._make(element["data"]),
            self.styler[f"Heading{level}"]
        )
        heading.keepWithNext = True
        return heading

    def _is_pdf(self, name):
        """test if filename end with .pdf"""
        n = name.lower()
        if n.endswith(".pdf"):
            return True
        else:
            return False

    def make_image(self, element):
        """image"""
        width = element["width"]
        height = element["height"]
        data = element["data"]
        align = element["align"].upper()

        _w = width * cm if width else None
        _h = height * cm if height else None
        if self._is_pdf(data):
            img = PdfImage(data, width=_w, height=_h)
        else:
            img = Image(data, width=_w, height=_h)

        img.hAlign = align

        return img

    def make_inline(self, element):
        """inline elements"""
        font = self.styler["Verbatim"].fontName
        return f"<font face='{font}'>{element['data']}</font>"

    def make_latex(self, element):
        """inline latex instructions"""
        warnings.warn(messages.MSG_0026)
        return Preformatted(element["data"], self.styler["Normal"])

    def make_line_break(self, element):
        """line break '<br/>'"""
        if element["data"]:
            h = element["data"] * cm
        else:
            h = 1 * cm
        return Spacer(0, h)

    def make_entry(self, element):

        def regularise(entries, style):
            """"merge text subsequent entries in a list and
            return them as a paragraph
            """
            out = []
            buf = []
            for e in entries:
                if isinstance(e, str):
                    buf.append(e)
                else:
                    if buf:
                        out.append(Paragraph(" ".join(buf), style=style))
                        buf = []
                    out.append(e)
            if buf:
                out.append(Paragraph(" ".join(buf), style=style))
            return out

        rv = [self.delegate(i) for i in element["data"]]
        rv = regularise(rv, self.styler["Normal"])
        if len(rv) == 1:
            return rv[0]
        else:
            return rv

    def make_list(self, element):
        """ordered and unordered lists"""

        # Styling
        if element["ordered"]:
            bt = "1"
            _style = "OrderedList"
        else:
            bt = "bullet"
            _style = "UnorderedList"

        items = [
            self.make_entry(i)
            for i in element["data"]
        ]

        lf = ListFlowable(
            items,
            bulletType=bt,
            style=self.styler[_style],
        )
        return lf

    def make_link(self, element):
        # not sure why this was required but it generated an error..
        # if not self.in_paragraph:
        #     raise DKitDocumentException(messages.MSG_0027, str(element))
        display = self._make(element["data"])
        address = f"<link href={element['url']}><u>{display}</u></link>"
        return address

    def make_block_quote(self, element):
        """block quotes"""
        t = self._make(element["data"][0]["data"])
        self.in_paragraph = True
        retval = Paragraph(t, self.styler["BlockQuote"])
        self.in_paragraph = False
        return retval

    def make_listing(self, element):
        """code listings"""
        return Preformatted(
            element["data"],
            self.styler["Code"]
        )

    def make_markdown(self, element):
        """convert from markdown"""
        transform = mistune.Markdown(renderer=JSONRenderer())
        content = transform(element["data"])
        return [self._make(e) for e in content]

    def make_paragraph(self, element):
        """paragraph"""
        self.in_paragraph = True
        retval = Paragraph(self._make(element["data"]), self.styler["BodyText"])
        self.in_paragraph = False
        return retval

    def make_text(self, element):
        """text"""
        return element["data"]

    def make_table(self, data):
        """generate table"""
        t = TableHelper(data, self.styler.local_style)
        table = Table(t.extract_data(), colWidths=t.widths(), repeatRows=1)
        table.setStyle(t.table_style())
        return table

    def make_verbatim(self, element):
        """verbtim text"""
        return Preformatted(
            element["data"],
            self.styler["Verbatim"]
        )

    def delegate(self, entry):
        """call appropriate callback for entry"""
        return self.callbacks[entry["~>"]](entry)

    def _make(self, items):
        if is_list(items):
            return "".join(self._make(i) for i in items)
        elif isinstance(items, str):
            return items
        else:
            return self.callbacks[items["~>"]](items)

    def _make_all(self):
        yield PageBreak()
        if isinstance(self.data, (Document, DictDocument)):
            content = self.data.as_dict()["elements"]
        else:
            content = self.data
        #  self.__write_debug_doc(content)
        for c in content:
            i = self.delegate(c)
            if is_list(i):
                yield from i
            else:
                yield i

    def __write_debug_doc(self, doc):
        """
        write the input document to json file

        for debugging purposes
        """
        import json
        with open("debug.json", "wt") as outfile:
            outfile.write(json.dumps(doc, indent=4))

    def __iter__(self):
        yield from self._make_all()


class ReportLabRenderer(object):
    """
    Reportlab Document Renderer
    """
    def __init__(self, styler=None, local_style=None):
        self.styler = styler if styler else RLStyler
        self.local_style = local_style

    def run(self, file_name, doc):
        self.styler = self.styler(doc, self.local_style)
        content = list(ReportlabDocRenderer(doc, self.styler))
        renderer = SimpleDocTemplate(
            file_name,
            pagesize=self.styler.page_size,
            rightMargin=self.styler.right_margin,
            leftMargin=self.styler.left_margin,
            topMargin=self.styler.top_margin,
            bottomMargin=self.styler.bottom_margin
        )
        renderer.build(
            content,
            onFirstPage=self.styler.first_page,
            onLaterPages=self.styler.later_pages
        )
