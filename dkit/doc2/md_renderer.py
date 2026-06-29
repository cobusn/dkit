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
Render dkit doc canonical format to Markdown (GitHub Flavored Markdown).

Tables use GFM pipe syntax.  Images render as standard Markdown image links.
PageBreak is rendered as a thematic break (``---``) since Markdown has no
native page-break concept.

Usage::

    renderer = MarkdownRenderer(document)
    renderer.render("output.md")        # write to file
    md = renderer.render_string()       # return as string
"""
import functools

from . import document as doc

_ALIGN_SEPARATOR = {
    "left": ":---",
    "right": "---:",
    "center": ":---:",
}


class MarkdownRenderer:
    """Render a Document to GitHub Flavored Markdown.

    Args:
        document: source Document object.
    """

    def __init__(self, document: doc.Document):
        self.doc = document

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render_string(self) -> str:
        """Return the rendered document as a string."""
        return self._make_elements(self.doc.elements)

    def render(self, file_name: str):
        """Write the rendered document to file_name.

        Args:
            file_name: destination path.
        """
        with open(file_name, "w", encoding="utf-8") as fh:
            fh.write(self.render_string())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_elements(self, elements) -> str:
        return "".join(self.make(e) for e in elements)

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    @functools.singledispatchmethod
    def make(self, element) -> str:
        raise TypeError(f"Unsupported element type: {type(element)}")

    @make.register(doc.Str)
    def make_str(self, element: doc.Str) -> str:
        return element.text

    @make.register(doc.Bold)
    def make_bold(self, element: doc.Bold) -> str:
        inner = self._make_elements(element.text)
        return f"**{inner}**"

    @make.register(doc.Emph)
    def make_emph(self, element: doc.Emph) -> str:
        inner = self._make_elements(element.text)
        return f"*{inner}*"

    @make.register(doc.Code)
    def make_code(self, element: doc.Code) -> str:
        return f"`{element.content}`"

    @make.register(doc.SoftBreak)
    def make_soft_break(self, element: doc.SoftBreak) -> str:
        return " "

    @make.register(doc.LineBreak)
    def make_line_break(self, element: doc.LineBreak) -> str:
        return "\n\n"

    @make.register(doc.HorizontalLine)
    def make_horizontal_line(self, element: doc.HorizontalLine) -> str:
        return "\n---\n\n"

    @make.register(doc.PageBreak)
    def make_page_break(self, element: doc.PageBreak) -> str:
        return "\n---\n\n"

    @make.register(doc.Link)
    def make_link(self, element: doc.Link) -> str:
        inner = self._make_elements(element.content)
        return f"[{inner}]({element.target})"

    @make.register(doc.Heading)
    def make_heading(self, element: doc.Heading) -> str:
        level = max(1, min(6, element.level))
        prefix = "#" * level
        if isinstance(element.content, list):
            text = self._make_elements(element.content)
        else:
            text = element.content
        return f"{prefix} {text}\n\n"

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph) -> str:
        inner = self._make_elements(element.content)
        return f"{inner}\n\n"

    @make.register(doc.Block)
    def make_block(self, element: doc.Block) -> str:
        inner = self._make_elements(element.content)
        return f"{inner}\n\n"

    @make.register(doc.BlockQuote)
    def make_block_quote(self, element: doc.BlockQuote) -> str:
        inner = self._make_elements(element.content)
        quoted = "\n".join(f"> {line}" for line in inner.splitlines())
        return f"{quoted}\n\n"

    @make.register(doc.CodeBlock)
    def make_code_block(self, element: doc.CodeBlock) -> str:
        lang = element.language or ""
        return f"```{lang}\n{element.content}\n```\n\n"

    @make.register(doc.List)
    def make_list(self, element: doc.List) -> str:
        depth = element.depth if element.depth is not None else 0
        indent = "  " * depth
        parts = []
        for i, item in enumerate(element.content):
            if element.ordered:
                marker = f"{i + 1}."
            else:
                marker = "-"
            item_text = self._make_list_item_text(item).rstrip("\n")
            parts.append(f"{indent}{marker} {item_text}\n")
        return "".join(parts) + "\n"

    def _make_list_item_text(self, element) -> str:
        """Render a ListItem's content as plain inline text."""
        if isinstance(element, doc.ListItem):
            return self._make_elements(element.content)
        return self.make(element)

    @make.register(doc.ListItem)
    def make_list_item(self, element: doc.ListItem) -> str:
        # ListItem is consumed directly by make_list; this handles the
        # edge case where a ListItem appears outside a List context.
        return self._make_elements(element.content)

    @make.register(doc.Image)
    def make_image(self, element: doc.Image) -> str:
        alt = element.title or ""
        return f"![{alt}]({element.source})\n\n"

    @make.register(doc.Table)
    def make_table(self, element: doc.Table) -> str:
        columns = element.columns

        # Header row
        headers = " | ".join(col.title for col in columns)
        header_row = f"| {headers} |"

        # Separator row with alignment markers
        seps = " | ".join(
            _ALIGN_SEPARATOR.get(col.align, "---") for col in columns
        )
        sep_row = f"| {seps} |"

        # Data rows
        data_rows = []
        for row in element.data:
            cells = " | ".join(col.formatter(row) for col in columns)
            data_rows.append(f"| {cells} |")

        return "\n".join([header_row, sep_row] + data_rows) + "\n\n"
