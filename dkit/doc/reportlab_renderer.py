# from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    Image,
    ListFlowable,
    ListItem,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
)
from svglib.svglib import svg2rlg
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from .document import AbstractRenderer
from ..utilities.introspection import is_list
# from ..plot import matplotlib as mpl
import mistune
from .json_renderer import JSONRenderer
from ..data.containers import AttrDict
from ..utilities.file_helper import temp_filename
from ..plot import matplotlib as mpl


class ReportlabDocRenderer(AbstractRenderer):

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

    def make_image(self, element):
        """image"""
        e = AttrDict(element)
        _w = e.width * cm if e.width else None
        _h = e.height * cm if e.height else None
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
