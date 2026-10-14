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
Render dkit doc cannonical format to Micosoft Word docx format using docx
"""
from . import document as doc
from . import math_helper
import docx
from docx.enum.section import WD_ORIENT
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml.shared import OxmlElement
from docx.shared import Cm
from .docx_helper import add_hyperlink, create_codeblock_style, DocxConfig
from dkit.stylepack.errors import StylePackError
from dkit.stylepack.model import StylePack
import functools
HEADING_COUNTER = 0


class DocxRenderer:
    """Render document elements to docx"""

    def __init__(self, document: doc.Document, allow_soft_breaks: bool = False,
                 config: DocxConfig = None, template: str = None,
                 style_pack: StylePack | None = None):
        if config is None:
            config = DocxConfig()
        self.allow_soft_breaks = allow_soft_breaks  # allow breaks in a paragraph
        self.doc = document
        self.config = config
        self.table_style = config.sty_table
        if style_pack is not None:
            formats = style_pack.manifest.formats.docx
            if formats is None:
                raise StylePackError(
                    f"style '{style_pack.manifest.id}' has no DOCX resources"
                )
            template = str(style_pack.resource(formats.template))
            if formats.table_style is not None:
                self.table_style = formats.table_style
        if template is None:
            self.xdoc = docx.Document()
        else:
            self.xdoc = docx.Document(template)
        if style_pack is not None:
            self._apply_style_pack(style_pack)
        # Create custom style
        create_codeblock_style(self.xdoc)

        # State variables
        self.current_paragraph = None
        self.current_style = self.config.sty_normal
        self.list_level = None  # used by lists to determine level
        self.list_type = None     # used by lists

    def _apply_style_pack(self, style_pack: StylePack):
        """Apply the pack template geometry to every document section.

        Args:
            style_pack: validated style pack supplying the DOCX template and
                physical page tokens.
        """
        formats = style_pack.manifest.formats.docx
        if formats is None:
            raise StylePackError(
                f"style '{style_pack.manifest.id}' has no DOCX resources"
            )
        template = style_pack.resource(formats.template)
        page_sizes = {
            "a4": (21.0, 29.7),
            "letter": (21.59, 27.94),
            "legal": (21.59, 35.56),
            "a5": (14.8, 21.0),
        }
        width, height = page_sizes[style_pack.manifest.page.size]
        page = style_pack.manifest.page
        if page.orientation == "landscape":
            width, height = height, width
        for section in self.xdoc.sections:
            section.orientation = (
                WD_ORIENT.LANDSCAPE
                if page.orientation == "landscape"
                else WD_ORIENT.PORTRAIT
            )
            section.page_width = Cm(width)
            section.page_height = Cm(height)
            section.left_margin = Cm(page.left_margin_cm)
            section.right_margin = Cm(page.right_margin_cm)
            section.top_margin = Cm(page.top_margin_cm)
            section.bottom_margin = Cm(page.bottom_margin_cm)

    def set_properties(self):
        """set document properties"""
        self.xdoc.core_properties.author = self.doc.author
        self.xdoc.core_properties.title = self.doc.title
        self.xdoc.core_properties.subject = self.doc.sub_title

    def make_elements(self, elements):
        for element in elements:
            self.make(element)

    @functools.singledispatchmethod
    def make(self, element):
        raise TypeError(f"Unsupported data type: {type(element)}")

    @make.register(doc.Code)
    def make_code(self, element: doc.Str):
        run = self.current_paragraph.add_run(element.content)
        return run

    @make.register(doc.Str)
    def make_str(self, element: doc.Str):
        return self.current_paragraph.add_run(element.text)

    @make.register(doc.Bold)
    def make_bold(self, element: doc.Bold):
        for elem in element.text:
            run = self.make(elem)
            run.font.bold = True

    @make.register(doc.Emph)
    def make_emph(self, element: doc.Emph):
        for elem in element.text:
            run = self.make(elem)
            run.font.italic = True

    @make.register(doc.List)
    def make_list(self, element: doc.List):
        buf = self.current_style
        self.list_level = element.depth + 1
        if element.ordered is True:
            self.list_type = self.config.sty_number_list
        else:
            self.list_type = self.config.sty_bullet
        self.current_style = self._get_bullet_style()
        for elem in element.content:
            self.make(elem)
        self.current_style = buf

    @make.register(doc.ListItem)
    def make_list_item(self, element: doc.ListItem):
        self._add_paragraph()
        self.make_elements(element.content)

    @make.register(doc.Block)
    def make_block(self, element: doc.Block):
        self.make_elements(element.content)

    def _add_paragraph(self, text="", style=None):
        if style is not None:
            self.current_style = style
        self.current_paragraph = self.xdoc.add_paragraph(
            text=text,
            style=self.current_style
        )
        return self.current_paragraph

    def _get_bullet_style(self):
        if self.list_level == 1:
            return f"List {self.list_type}"
        elif self.list_level > 1:
            return f"List {self.list_type} {self.list_level}"
        else:
            raise ValueError(f"bullet level {self.list_level} is invalid")

    @make.register(doc.Heading)
    def make_heading(self, element: doc.Heading):
        self.current_paragraph = self.xdoc.add_heading(
            "",
            element.level
        )
        self.make_elements(element.content)

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph):
        self.current_paragraph = self._add_paragraph()
        self.make_elements(element.content)

    @make.register(doc.LineBreak)
    def make_line_break(self, element: doc.LineBreak):
        self.current_paragraph = self.xdoc.add_paragraph()
        """
        run = self.xdoc.add_paragraph().add_run()
        run.add_break()
        """

    @make.register(doc.Link)
    def make_hyperlink(self, element: doc.Link):
        add_hyperlink(
            self.current_paragraph,
            self._reduce_to_text(element.content),
            element.target
        )

    def _reduce_to_text(self, elements) -> str:
        """flatten inline elements to plain text, for use by hyperlinks etc

        Link content is not always one plain Str: text wrapped across a
        source line becomes Str/SoftBreak/Str, and Bold/Emph hold their own
        text as a further list rather than a string -- both need flattening
        rather than a bare ``.text`` lookup.
        """
        parts = []
        for element in elements:
            if isinstance(element, doc.SoftBreak):
                parts.append(" ")
            elif isinstance(element, (doc.Bold, doc.Emph)):
                parts.append(self._reduce_to_text(element.text))
            elif isinstance(element, doc.Str):
                parts.append(element.text)
            else:
                raise TypeError(f"Only inline text elements expected here: {element!r}")
        return "".join(parts)

    @make.register(doc.SoftBreak)
    def make_soft_break(self, element: doc.SoftBreak):
        if self.allow_soft_breaks:
            run = self.current_paragraph.add_run()
            run.add_break()

    @make.register(doc.PageBreak)
    def make_page_break(self, element: doc.PageBreak):
        self.xdoc.add_page_break()

    @make.register(doc.CodeBlock)
    def make_code_block(self, element: doc.CodeBlock):
        buffer = self.current_style  # Save for later"
        self._add_paragraph(
            element.content,
            style=self.config.sty_code_block
        )
        self._add_paragraph(style=buffer)

    @make.register(doc.BlockQuote)
    def make_block_quote(self, element: doc.BlockQuote):
        buffer = self.current_style  # Save for later"
        self.current_style = self.config.sty_quote
        self.make_elements(element.content)
        self.current_style = buffer

    @make.register(doc.Image)
    def make_image(self, element: doc.Image):
        para = self._add_paragraph()
        run = para.add_run()
        run.add_picture(
            element.source,
            width=Cm(element.width) if element.width else None,
            height=Cm(element.height) if element.height else None
        )
        para.alignment = self._translate_alignment(element.align)

    @make.register(doc.Math)
    def make_math(self, element: doc.Math):
        # rendered raster (PNG): docx has no clean vector math path
        # (OMML conversion and SVG dual-rendition embedding are both
        # out of scope -- see _specs/math.md)
        path = math_helper.render_math_file(element, "png")
        if element.display:
            para = self._add_paragraph()
            run = para.add_run()
            run.add_picture(path)
            para.alignment = self._translate_alignment(element.align)
        else:
            # stays in the current paragraph's run sequence, same as
            # make_str/make_bold -- a new paragraph breaks inline math
            # onto its own line (same bug class fixed for ReportLab,
            # see _specs/math.md)
            run = self.current_paragraph.add_run()
            run.add_picture(path)
            self._raise_run(run, math_helper.inline_descent_pt(element))

    def _raise_run(self, run, descent_pt: float):
        """Lower a run by descent_pt points, via raw w:position XML.

        Word positions an inline picture's bottom edge on the text
        baseline (same convention as HTML's default inline <img>
        vertical-align) -- but render_math_file crops each formula
        tightly to its own ink, so a formula with a descender (e.g.
        \\phi's loop) has its own content baseline sitting above the
        image's bottom edge by descent_pt, which otherwise floats the
        formula above the surrounding text (same symptom fixed for
        ReportLab -- see _specs/math.md). python-docx exposes no
        property for this, hence the raw w:position element.
        """
        r_pr = run._element.get_or_add_rPr()
        position = OxmlElement("w:position")
        # w:position is in half-points; negative lowers the run
        position.set(qn("w:val"), str(-round(descent_pt * 2)))
        r_pr.append(position)

    def _translate_alignment(self, align: str):
        _align = align.lower()
        match _align:
            case "left":
                return WD_ALIGN_PARAGRAPH.LEFT
            case "right":
                return WD_ALIGN_PARAGRAPH.RIGHT
            case "center":
                return WD_ALIGN_PARAGRAPH.CENTER
        return WD_ALIGN_PARAGRAPH.CENTER

    def _translate_table_alignment(self, align: str):
        _align = align.lower()
        match _align:
            case "left":
                return WD_TABLE_ALIGNMENT.LEFT
            case "right":
                return WD_TABLE_ALIGNMENT.RIGHT
            case "center":
                return WD_TABLE_ALIGNMENT.CENTER
        return WD_TABLE_ALIGNMENT.CENTER

    @make.register(doc.Table)
    def make_table(self, element: doc.Table):
        rows = len(element.data) + 1
        cols = len(element.columns)
        table = self.xdoc.add_table(rows, cols, style=self.table_style)
        table.autofit = False
        table.alignment = self._translate_table_alignment(element.align)

        # set table column widths
        for i, col in enumerate(element.columns):
            table.columns[i].width = Cm(col.width)

        # set headers
        headers = table.rows[0].cells
        for i, col in enumerate(element.columns):
            header = headers[i]
            header.text = col.title
            header.width = Cm(col.width)
            for p in header.paragraphs:
                p.alignment = self._translate_alignment(col.heading_align)

        # set data
        for i, data_row in enumerate(element.data):
            row = table.rows[i + 1]
            for j, col in enumerate(element.columns):
                cell = row.cells[j]
                cell.text = str(data_row[col.name])
                cell.width = col.width
                for p in cell.paragraphs:
                    p.alignment = self._translate_alignment(col.align)

    def render(self, file_name: str):
        self.set_properties()
        self.make_elements(self.doc.elements)
        self.xdoc.save(file_name)
