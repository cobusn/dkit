# from reportlab.lib.enums import TA_JUSTIFY
import mistune
from pdfrw import PdfReader
from pdfrw.buildxobj import pagexobj
from pdfrw.toreportlab import makerl
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image, ListFlowable, ListItem, Paragraph, SimpleDocTemplate, Spacer, Flowable
)
from svglib.svglib import svg2rlg

from ..data.containers import AttrDict
from ..plot import matplotlib as mpl
from ..utilities.file_helper import temp_filename
from ..utilities.introspection import is_list
from .document import AbstractRenderer
from .json_renderer import JSONRenderer


# from ..plot import matplotlib as mpl


class PdfImage(Flowable):
    """
    PdfImage wraps the first page from a PDF file as a Flowable which can
    be included into a ReportLab Platypus document.
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


class ReportlabDocRenderer(AbstractRenderer):
    """
    Render a cannonical json like formatted document
    to pdf using the Reportlab library.

    Although the Latex version will produce better
    layouts, this version is useful for generating
    pdf documents without littering the filesystem
    with tex files.
    """

    def __init__(self, data, stylesheet, plot_stylesheet):
        super().__init__(data)
        self.style = stylesheet
        self.plot_backend = mpl.MPLBackend
        self.plot_style = plot_stylesheet

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
        filename = str(temp_filename(suffix="svg"))
        be = self.plot_backend(
            data,
            terminal="svg",
            style_sheet=self.plot_style
        )
        be.render(filename)
        drawing = svg2rlg(filename)
        return drawing

    def make_heading(self, element):
        """format heading"""
        level = element["level"]
        return Paragraph(
            self._make(element["data"]),
            self.style[f"Heading{level}"]
        )

    def _is_pdf(self, name):
        """test if filename end with .pdf"""
        n = name.lower()
        if n.endswith(".pdf"):
            return True
        else:
            return False

    def make_image(self, element):
        """image"""
        e = AttrDict(element)
        _w = e.width * cm if e.width else None
        _h = e.height * cm if e.height else None
        if self._is_pdf(e.data):
            img = PdfImage(e.data, width=_w, height=_h)
        else:
            img = Image(e.data, width=_w, height=_h)
        img.hAlign = e.align.upper()
        return img

    def make_inline(self, element):
        pass

    def make_latex(self, data):
        pass

    def make_line_break(self, element):
        """line break '<br/>'"""
        if element["data"]:
            h = element["data"] * cm
        else:
            h = 1 * cm
        return Spacer(0, h)

    def make_entry(self, element):
        if isinstance(element["data"], dict) and element["data"]["~>"] == "list":
            return self.make_list(element["data"])
        else:
            return ListItem(self.make_paragraph(element))

    def make_list(self, element):
        """ordered and unordered lists"""
        if element["ordered"]:
            bt = "1"
        else:
            bt = "bullet"
        items = [
            self.make_entry(i)
            for i in element["data"]
        ]
        return ListFlowable(
            items,
            bulletType=bt
        )

    def make_link(self, element):
        pass

    def make_block_quote(self, element):
        pass

    def make_listing(self, element):
        pass

    def make_markdown(self, element):
        """convert from markdown"""
        transform = mistune.Markdown(renderer=JSONRenderer())
        return [self._make(e) for e in transform(element["data"])]

    def make_paragraph(self, element):
        """paragraph"""
        return Paragraph(self._make(element["data"]), self.style["Normal"])

    def make_text(self, element):
        """text"""
        return element["data"]

    def make_table(self, data):
        pass

    def make_verbatim(self, data):
        pass

    def _make(self, item):
        if is_list(item):
            return "".join(self._make(i) for i in item)
        elif isinstance(item, str):
            return item
        else:
            return self.callbacks[item["~>"]](item)

    def _make_all(self):
        for c in self.data.as_dict()["elements"]:
            i = self.callbacks[c["~>"]](c)
            if is_list(i):
                yield from i
            else:
                yield i

    def __iter__(self):
        yield from self._make_all()


class ReportLabBuilder(object):

    def __init__(self, pagesize=A4, left_margin=25, right_margin=25,
                 top_margin=25, bottom_margin=25):
        self.pagesize = pagesize
        self.left_margin = left_margin
        self.right_margin = right_margin
        self.top_margin = top_margin
        self.bottom_margin = bottom_margin

    def run(self, file_name, content):
        doc = SimpleDocTemplate(
            file_name,
            pagesize=self.pagesize,
            rightMargin=self.right_margin,
            leftMargin=self.right_margin,
            topMargin=self.top_margin,
            bottomMargin=self.bottom_margin
        )
        doc.build(content)
