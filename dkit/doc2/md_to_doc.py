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
Render markdown to dkit document format
"""
from textwrap import indent
from typing import Any, Dict, Iterable, cast

import mistune
from mistune.core import BaseRenderer, BlockState
from . import document as doc


class DocRenderer(BaseRenderer):
    """A renderer to re-format Markdown text."""
    NAME = 'markdown'

    def __init__(self):
        super().__init__()

    def __call__(self, tokens: Iterable[Dict[str, Any]], state: BlockState) -> str:
        out = self.render_tokens(tokens, state)
        return out

    def render_tokens(self, tokens: Iterable[Dict[str, Any]], state: BlockState) -> str:
        ls = list(self.iter_tokens(tokens, state))
        return ls

    def render_children(self, token: Dict[str, Any], state: BlockState) -> str:
        children = token['children']
        return self.render_tokens(children, state)

    def softbreak(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.SoftBreak()

    def blank_line(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.LineBreak()

    def text(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.Str(cast(str, token["raw"]))

    def emphasis(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.Emph(self.render_children(token, state))

    def strong(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.Bold(self.render_children(token, state))

    def paragraph(self, token: Dict[str, Any], state: BlockState) -> str:
        children = self.render_children(token, state)
        return doc.Paragraph(children)

    def heading(self, token: Dict[str, Any], state: BlockState) -> str:
        level = cast(int, token["attrs"]["level"])
        text = self.render_children(token, state)
        return doc.Heading(text, level)

    def link(self, token: Dict[str, Any], state: BlockState) -> str:
        content = self.render_children(token, state)
        target = token['attrs']['url']
        return doc.Link(content, target)

    def image(self, token: Dict[str, Any], state: BlockState) -> str:
        title = self.render_children(token, state)
        target = token['attrs']['url']
        return doc.Image(
            target,
            title,
        )

    def list(self, token: Dict[str, Any], state: BlockState) -> str:
        ordered = token["attrs"]["ordered"]
        content = self.render_children(token, state)
        depth = token["attrs"].get("depth")
        return doc.List(content, ordered, depth)

    def list_item(self, token: Dict[str, Any], state: BlockState) -> str:
        content = self.render_children(token, state)
        return doc.ListItem(content)

    def block_text(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.Block(self.render_children(token, state))

    def special(self, token: Dict[str, Any], state: BlockState) -> str:
        content = token["raw"]
        obj = doc.from_json(content)
        return obj

    def block_code(self, token: Dict[str, Any], state: BlockState) -> str:
        content = token["raw"]
        if "attrs" in token:
            lang = token["attrs"].get("info", None)
        else:
            lang = None
        if lang == "jsoninclude":
            return self.special(token, state)
        return doc.CodeBlock(
            content=content,
            language=lang
        )

    def codespan(self, token: Dict[str, Any], state: BlockState) -> str:
        """inline code"""
        return doc.Code(token["raw"])

    def block_quote(self, token: Dict[str, Any], state: BlockState) -> str:
        content = self.render_children(token, state)
        return doc.BlockQuote(content)

    def linebreak(self, token: Dict[str, Any], state: BlockState) -> str:
        raise Exception(f"linebreak not yet defined: {token}")

    def inline_html(self, token: Dict[str, Any], state: BlockState) -> str:
        if token["raw"] == "<locals>":
            raise Exception(f"<locals> tag exception raised.  Check that decorators have parameters where required")
        raise Exception(f"Inline HTML not supported: {token['raw']}")

    def thematic_break(self, token: Dict[str, Any], state: BlockState) -> str:
        return doc.HorizontalLine()

    def block_html(self, token: Dict[str, Any], state: BlockState) -> str:
        raise Exception(f"Inline HTML not supported: {token['raw']}")

    def block_error(self, token: Dict[str, Any], state: BlockState) -> str:
        raise NotImplementedError()

    def render_referrences(self, state: BlockState) -> Iterable[str]:
        # review references documentation from Mistune
        raise NotImplementedError()


def render_doc_format(markdown):
    """Render mardown to Doc format"""
    renderer = DocRenderer()
    return mistune.create_markdown(renderer=renderer)(markdown)
