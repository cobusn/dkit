# Copyright (c) 2019 Cobus Nel
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
Mistune Renderer that will render JSON document from markdown

=========== =============== =================================================
11 Jul 2019 Cobus Nel       Created
=========== =============== =================================================
"""

import mistune
from dkit.data import json_utils as ju
from . import document


json = ju.JsonSerializer(
    ju.DateTimeCodec(),
    ju.DateCodec(),
    ju.Decimal2FloatCodec(),
    ju.PandasTimestampCodec(),
)


class JSONRenderer(mistune.Renderer):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.prevent_latex_linebreak = True

    def placeholder(self):
        return []

    def autolink(self, link, is_email):
        """link"""
        if is_email:
            link = link.strip("mailto:")
        return [document.AutoLink(link, is_email).as_dict()]

    def block_code(self, code, lang):
        """Code listing"""
        # latex to be passed verbatim
        if lang and lang == 'texinclude':
            return [document.Latex(code).as_dict()]

        # json to be pased verbatim
        elif lang and lang == 'jsoninclude':
            return [json.loads(code)]

        # no language
        elif lang is not None:
            return [document.Listing(code, lang).as_dict()]

        # use listings environment when language is defined
        else:
            return [document.Verbatim(code).as_dict()]

    def block_quote(self, text):
        """Block quote"""
        return [document.BlockQuote(text).as_dict()]

    def block_html(self, html):
        raise NotImplementedError

    def text(self, text):
        return [document.Text(text).as_dict()]

    def codespan(self, text):
        """inline code"""
        return [document.Inline(text).as_dict()]

    def double_emphasis(self, text):
        """bold"""
        return [document.Bold(text).as_dict()]

    def emphasis(self, text):
        """italics"""
        return [document.Emphasis(text).as_dict()]

    def footnotes(self, items):
        return items

    def footnote_item(self, key, text):
        return [document.FootnoteItem(text, key).as_dict()]

    def footnote_ref(self, key, index):
        return [document.FootnoteRef(index, key).as_dict()]

    def link(self, link, title, text):
        """link"""
        return [document.Link(text, link, title).as_dict()]

    def header(self, text, level, raw):
        """
        Header
        """
        rv = [document.Heading(text, level).as_dict()]
        return rv

    def inline_html(self, html):
        raise NotImplementedError

    def escape(text):
        raise NotImplementedError

    def image(self, src, title, text):
        """image"""
        _title = title if title else text
        return [
            document.Image(
                src,
                _title,
                align="center"
            ).as_dict()
        ]

    def list(self, body, ordered):
        rv = document.List(ordered)
        for entry in body:
            rv.add_entry(entry)
        return [rv.as_dict()]

    def list_item(self, body):
        return [document.List.Entry(body).as_dict()]

    def linebreak(self):
        return [document.LineBreak(0).as_dict()]

    def newline(self):
        return [document.LineBreak(0).as_dict()]

    def hrule(self):
        return [document.HRule().as_dict()]

    def paragraph(self, content):
        # Images are parsed as part of a paragraph by mistune
        # at this point the json format assumes that all para
        # content are text, so return image instead in this case
        if content[0]["~>"] == "image":
            return content
        else:
            p = document.Paragraph(content)
            return [p.as_dict()]

    def table_cell(self, content, **flags):
        return content

    def table_row(self, content):
        return [content]

    def table(self, header, body):
        return []
