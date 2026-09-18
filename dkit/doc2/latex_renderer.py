# Copyright (c) 2026 Cobus Nel
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
Render the dkit doc2 canonical format to a LaTeX .tex file.
"""

import functools
from pathlib import Path

from . import document as doc
from . import latex as tex
from dkit.stylepack.errors import StylePackError
from dkit.stylepack.model import StylePack

__all__ = ["LatexRenderer"]


class LatexRenderer:
    """Render a doc2 Document to a LaTeX source file.

    Uses singledispatchmethod to handle each canonical element type, following
    the same pattern as RLRenderer and DocxRenderer.

    Heading levels map to LaTeX sectioning commands via section_map. The
    default maps level 1 to section, which suits the article document class.
    Override section_map in a subclass when using report or book classes that
    have chapter.

    Args:
        document: the canonical document to render.
        doc_type: LaTeX document class (default article).
        font_size: base font size in pt; 10, 11, or 12 (default 11).
        paper_size: paper size (default a4paper).
        styler: accepted for API compatibility with Builder; unused.
    """

    section_map = {
        1: tex.Section,
        2: tex.SubSection,
        3: tex.SubSubSection,
    }

    def __init__(
        self,
        document: doc.Document,
        doc_type: str = "article",
        font_size: int = 11,
        paper_size: str = "a4paper",
        styler=None,          # accepted for API compatibility; unused
        style_pack: StylePack | None = None,
    ):
        if style_pack is not None:
            formats = style_pack.manifest.formats.latex
            if formats is None:
                raise StylePackError(
                    f"style '{style_pack.manifest.id}' has no LaTeX resources"
                )
            if doc_type == "article":
                doc_type = formats.class_name
            paper_size = _paper_option(style_pack)
            page = style_pack.manifest.page
            geometry_options = ",".join([
                f"left={page.left_margin_cm}cm",
                f"right={page.right_margin_cm}cm",
                f"top={page.top_margin_cm}cm",
                f"bottom={page.bottom_margin_cm}cm",
            ])
        else:
            geometry_options = None
        self.document = document
        self._tex_doc = tex.Document(
            doc_type=doc_type,
            font_size=font_size,
            paper_size=paper_size,
            geometry_options=geometry_options,
        )
        self._set_title_fields()

    def _set_title_fields(self):
        """put the document's title fields into the LaTeX preamble

        \\title{}/\\author{}/\\date{} are standard across every class, built
        in or custom, since even a custom class only ever ``\\LoadClass``s
        one of the built ins. sub_title/contact/version have no LaTeX
        equivalent, so each goes in as a \\providecommand immediately
        followed by a \\renewcommand: \\providecommand alone would silently
        lose to a class that already defines the name with its own default
        (e.g. for standalone use without dkit) -- it only takes effect when
        the name is *not yet* defined, and by then the class has already
        claimed it. \\renewcommand needs the name to already exist, which
        the \\providecommand immediately before it guarantees either way.

        \\maketitle itself is queued as ``title_command`` rather than
        appended here: it has to run after \\begin{document}, and every
        class is expected to define what it does -- the same hook
        ``article``/``report``/``book`` already use -- rather than dkit
        needing to know a class-specific command name.
        """
        d = self.document
        if d.title:
            self._tex_doc.preamble_extra.append(f"\\title{{{tex.encode(d.title)}}}")
        if d.author:
            self._tex_doc.preamble_extra.append(f"\\author{{{tex.encode(d.author)}}}")
        date = d._title_date or d._date.strftime("%Y-%m-%d")
        self._tex_doc.preamble_extra.append(f"\\date{{{tex.encode(date)}}}")
        for name, value in (("subtitle", d.sub_title), ("contact", d.contact),
                            ("version", d.version)):
            if value:
                self._tex_doc.preamble_extra.append(f"\\providecommand{{\\{name}}}{{}}")
                self._tex_doc.preamble_extra.append(
                    f"\\renewcommand{{\\{name}}}{{{tex.encode(value)}}}"
                )
        if d.title:
            self._tex_doc.title_command = "\\maketitle"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_text(self, elements) -> str:
        """Reduce a list of inline elements to a plain LaTeX string.

        Args:
            elements: list of inline doc elements.

        Returns:
            Concatenated LaTeX string.
        """
        return "".join(self._inline(e) for e in elements)

    def _inline(self, element) -> str:
        """Convert a single inline element to a LaTeX string.

        Args:
            element: an inline doc element (Str, Bold, Emph, Code, Link, SoftBreak).

        Returns:
            LaTeX string representation of the element.
        """
        if isinstance(element, doc.Str):
            return tex.encode(element.text)
        if isinstance(element, doc.Bold):
            return str(tex.Bold(self._make_text(element.text)))
        if isinstance(element, doc.Emph):
            return str(tex.Emph(self._make_text(element.text)))
        if isinstance(element, doc.Code):
            return str(tex.Inline(element.content))
        if isinstance(element, doc.Link):
            return str(tex.Href(element.target, self._make_text(element.content)))
        if isinstance(element, doc.SoftBreak):
            return " "
        return tex.encode(str(element))

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def make_elements(self, elements):
        """Iterate over elements and dispatch each to its make handler.

        Args:
            elements: list of canonical doc elements.
        """
        for element in elements:
            self.make(element)

    @functools.singledispatchmethod
    def make(self, element):
        raise TypeError(f"unsupported element type: {type(element)}")

    @make.register(doc.Str)
    def make_str(self, element: doc.Str):
        self._tex_doc.append(tex.Paragraph(element.text))

    @make.register(doc.SoftBreak)
    def make_soft_break(self, element: doc.SoftBreak):
        pass  # soft breaks are collapsed to a space in inline context

    @make.register(doc.LineBreak)
    def make_line_break(self, element: doc.LineBreak):
        self._tex_doc.append(tex.LineBreak())

    @make.register(doc.PageBreak)
    def make_page_break(self, element: doc.PageBreak):
        self._tex_doc.append(tex.NewPage())

    @make.register(doc.HorizontalLine)
    def make_horizontal_line(self, element: doc.HorizontalLine):
        self._tex_doc.append(tex.Latex(r"\noindent\rule{\textwidth}{0.4pt}"))

    @make.register(doc.Heading)
    def make_heading(self, element: doc.Heading):
        text = self._make_text(element.content)
        cls = self.section_map.get(element.level, tex.SubSubSection)
        self._tex_doc.append(cls(text))

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph):
        # native markdown image syntax (![alt](src)) nests the Image inside
        # the paragraph's own content, unlike the {{ image(...) }} jinja
        # helper's Image, which is always its own top-level element --
        # _make_text has no case for Image and would silently encode its
        # repr as literal text, so any embedded Image is dispatched on its
        # own instead, via the existing make_image handler
        inlines = []
        for item in element.content:
            if isinstance(item, doc.Image):
                if inlines:
                    self._tex_doc.append(tex.Paragraph(self._make_text(inlines)))
                    inlines = []
                self.make(item)
            else:
                inlines.append(item)
        if inlines:
            self._tex_doc.append(tex.Paragraph(self._make_text(inlines)))

    @make.register(doc.Block)
    def make_block(self, element: doc.Block):
        text = self._make_text(element.content)
        self._tex_doc.append(tex.Paragraph(text))

    @make.register(doc.BlockQuote)
    def make_block_quote(self, element: doc.BlockQuote):
        text = self._flatten_paragraphs(element.content)
        self._tex_doc.append(tex.BlockQuote(text))

    def _flatten_paragraphs(self, content) -> str:
        """inline text from a mixed list of inline elements and Paragraphs

        A blockquote with a blank line inside becomes more than one
        Paragraph in its content (mistune's parse shape) rather than a flat
        run of inline elements -- the same shape nested lists have, and the
        same bug if it is missed: treating a Paragraph as inline text falls
        through to its Python repr instead of its own text.
        """
        paragraphs = []
        inlines = []
        for item in content:
            if isinstance(item, doc.Paragraph):
                if inlines:
                    paragraphs.append(self._make_text(inlines))
                    inlines = []
                paragraphs.append(self._flatten_paragraphs(item.content))
            else:
                inlines.append(item)
        if inlines:
            paragraphs.append(self._make_text(inlines))
        return "\n\n".join(paragraphs)

    @make.register(doc.Bold)
    def make_bold(self, element: doc.Bold):
        text = self._make_text(element.text)
        self._tex_doc.append(tex.Paragraph(str(tex.Bold(text))))

    @make.register(doc.Emph)
    def make_emph(self, element: doc.Emph):
        text = self._make_text(element.text)
        self._tex_doc.append(tex.Paragraph(str(tex.Emph(text))))

    @make.register(doc.Link)
    def make_link(self, element: doc.Link):
        text = self._make_text(element.content)
        self._tex_doc.append(tex.Href(element.target, text))

    @make.register(doc.Image)
    def make_image(self, element: doc.Image):
        # resolved to an absolute path: pdflatex compiles with its cwd set
        # to the output directory (see Builder._compile_latex), which is not
        # necessarily the directory a relative source path is relative to
        source = str(Path(element.source).resolve())
        self._tex_doc.append(
            tex.Image(
                source,
                width=element.width,
                height=element.height,
                alignment=element.align,
            )
        )

    @make.register(doc.Code)
    def make_code(self, element: doc.Code):
        self._tex_doc.append(tex.Inline(element.content))

    @make.register(doc.CodeBlock)
    def make_code_block(self, element: doc.CodeBlock):
        if element.language == "texinclude":
            # raw LaTeX passthrough: a way to drop custom LaTeX straight
            # into a document when the canonical format has no construct
            # for it. Appended as a plain string rather than through
            # tex.Listing/tex.Latex -- this content is already valid LaTeX,
            # and encode() would corrupt exactly the characters (_, ^, %,
            # &, $) real LaTeX commands need.
            self._tex_doc.append(f"\n{element.content}\n")
            return
        # None/omitted rather than a "text" default: the listings package has
        # no such language, and omitting language= just disables highlighting
        self._tex_doc.append(tex.Listing(element.content, element.language))

    def _flatten_item(self, item: doc.ListItem) -> str:
        """Extract inline text from a list item's block containers.

        A nested sub-list is a ``doc.List`` sibling within the item's own
        content (mistune's parse shape), not inline text -- rendering it as
        text via the generic ``hasattr(block, "content")`` branch below would
        extend with raw ``ListItem`` objects rather than their text, so it is
        recursed into its own nested ``\\begin{itemize}``/``\\begin{enumerate}``
        instead and the two pieces are joined.

        Args:
            item: a ListItem whose content may be wrapped in Block elements.

        Returns:
            Plain LaTeX string of the item text.
        """
        parts = []
        inlines = []
        for block in item.content:
            if isinstance(block, doc.List):
                if inlines:
                    parts.append(self._make_text(inlines))
                    inlines = []
                parts.append(str(self._render_list(block)))
            elif hasattr(block, "content"):
                inlines.extend(block.content)
            else:
                inlines.append(block)
        if inlines:
            parts.append(self._make_text(inlines))
        return "".join(parts)

    def _render_list(self, element: doc.List):
        """Build a tex Itemize/Enumerate from a doc.List, recursing into it.

        Args:
            element: the list to render.

        Returns:
            A populated :class:`tex.Itemize` or :class:`tex.Enumerate`.
        """
        lst = tex.Enumerate() if element.ordered else tex.Itemize()
        for item in element.content:
            if isinstance(item, doc.ListItem):
                lst.append(tex.Item(self._flatten_item(item)))
        return lst

    @make.register(doc.List)
    def make_list(self, element: doc.List):
        self._tex_doc.append(self._render_list(element))

    @make.register(doc.ListItem)
    def make_list_item(self, element: doc.ListItem):
        # bare ListItem outside a List — handled gracefully
        self._tex_doc.append(tex.Item(self._flatten_item(element)))

    @staticmethod
    def _field_spec(column) -> dict:
        """a doc2 Column or SparkLine as the field-spec dict LongTable reads

        LongTable predates the typed Column/SparkLine model and still speaks
        the older ``{"~>": ...}`` shape; this is the adapter between them.
        """
        if isinstance(column, doc.SparkLine):
            return {
                "~>": "sparkline",
                "spark_data": column.spark_data,
                "master": column.master,
                "child": column.child,
                "value": column.value,
                "title": column.title,
                "width": column.width,
                "height": column.height,
                "align": column.align,
                "heading_align": column.heading_align,
            }
        return {
            "~>": "field",
            "name": column.name,
            "title": column.title,
            "width": column.width,
            "align": column.align,
            "heading_align": column.heading_align,
            "format_": column.format_,
        }

    @make.register(doc.Table)
    def make_table(self, element: doc.Table):
        # Always LongTable, matching v1's make_table: a table can outgrow
        # one page, and SimpleTable's plain tabulate-generated tabular has
        # no page-break support and no styled header row, unlike LongTable's
        # \rowcolor(tableheader)/\textcolor{tabletextcolor}. SparkLine
        # columns need it regardless -- they have no per-cell formatter(),
        # since they draw from a separate row history rather than one value.
        field_spec = [self._field_spec(col) for col in element.columns]
        self._tex_doc.append(tex.LongTable(element.data, field_spec,
                                            align=element.align))

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def render(self, file_name: str):
        """Render the document and write LaTeX source to file.

        Args:
            file_name: output path for the .tex file.
        """
        self.make_elements(self.document.elements)
        with open(file_name, "wt", encoding="utf-8") as fh:
            fh.write(str(self._tex_doc))


def _paper_option(style_pack: StylePack) -> str:
    """Translate shared page tokens into LaTeX class options."""
    sizes = {
        "a4": "a4paper",
        "letter": "letterpaper",
        "legal": "legalpaper",
        "a5": "a5paper",
    }
    option = sizes[style_pack.manifest.page.size]
    if style_pack.manifest.page.orientation == "landscape":
        option += ",landscape"
    return option
