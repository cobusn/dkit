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
Render dkit doc canonical format to HTML.

All block and inline elements are annotated with CSS classes so consumers
can apply stylesheets without modifying this renderer.  No inline styles are
emitted except per-cell table alignment, which cannot be expressed in a
single class on the column.

Usage::

    # Web output (class-based HTML)
    renderer = HtmlRenderer(document, css="styles.css")
    renderer.render("output.html")
    html = renderer.render_string()

    # Email output (CSS inlined, images embedded)
    renderer = HtmlRenderer(document, css="email.css", inline_images=True)
    html = renderer.render_email_string()   # ready for SmtpMessage(html_body=...)
"""
import base64
import functools
import html as _html
import mimetypes

from . import document as doc


class HtmlRenderer:
    """Render a Document to HTML.

    Args:
        document: source Document object.
        fragment: when True, emit only the body content without the outer
            ``<!DOCTYPE html>`` wrapper and ``<head>`` block.  Useful for
            embedding the output in a larger page.
        lang: BCP-47 language tag placed on the ``<html>`` element
            (full-document mode only).
        css: optional CSS to inject into ``<head>``.  Pass a raw CSS string
            or a path ending in ``.css`` / ``.CSS`` to load from a file.
        inline_images: when True, local image file paths are converted to
            base64 data URIs so images are embedded in the HTML document.
            Remote URLs (``http://`` / ``https://``) are always left as-is.
    """

    def __init__(
        self,
        document: doc.Document,
        fragment: bool = False,
        lang: str = "en",
        css: str | None = None,
        inline_images: bool = False,
    ):
        self.doc = document
        self.fragment = fragment
        self.lang = lang
        self._inline_images = inline_images
        self._css = self._load_css(css)

    @staticmethod
    def _load_css(css: str | None) -> str | None:
        """Return CSS as a string, loading from file when css is a .css path."""
        if css is None:
            return None
        if css.lower().endswith(".css"):
            with open(css, "r", encoding="utf-8") as fh:
                return fh.read()
        return css

    def _to_data_uri(self, path: str) -> str:
        """Return a base64 data URI for a local image file.

        Args:
            path: filesystem path to the image.

        Returns:
            A data URI string of the form ``data:<mime>;base64,<data>``.
        """
        mime, _ = mimetypes.guess_type(path)
        mime = mime or "image/png"
        with open(path, "rb") as fh:
            data = base64.b64encode(fh.read()).decode()
        return f"data:{mime};base64,{data}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render_string(self) -> str:
        """Return the rendered document as a string."""
        body = self._make_elements(self.doc.elements)
        if self.fragment:
            return body
        return self._wrap_document(body)

    def render(self, file_name: str):
        """Write the rendered document to file_name.

        Args:
            file_name: destination path.
        """
        with open(file_name, "w", encoding="utf-8") as fh:
            fh.write(self.render_string())

    def render_email_string(self) -> str:
        """Return email-safe HTML with CSS inlined and images embedded.

        All CSS class rules from the stylesheet supplied via ``css`` are
        converted to inline ``style=""`` attributes so that email clients
        that strip ``<style>`` blocks (Gmail, many mobile clients) still
        render the document correctly.

        Local images are embedded as base64 data URIs when
        ``inline_images=True`` was passed to the constructor, ensuring
        banners and logos display without remote-image blocking.

        Requires ``premailer`` (``pip install premailer``).

        Returns:
            HTML string suitable for ``SmtpMessage(html_body=...)``.
        """
        import premailer
        # Run premailer on the class-based HTML (file paths, not data URIs)
        # so it never sees a data: src and cannot strip the <img> tags.
        html = premailer.transform(self.render_string())
        # Embed local images as data URIs after CSS inlining.
        if self._inline_images:
            html = self._embed_images_in_html(html)
        return html

    def _embed_images_in_html(self, html: str) -> str:
        """Replace local file-path src attributes with base64 data URIs.

        Args:
            html: HTML string containing ``src="<path>"`` attributes.

        Returns:
            HTML string with local paths replaced by data URIs.
        """
        import re
        def _replace(match):
            src = match.group(1)
            if src.startswith(("http://", "https://", "data:", "cid:")):
                return match.group(0)
            try:
                data_uri = self._to_data_uri(src)
                return f'src="{data_uri}"'
            except OSError:
                return match.group(0)
        return re.sub(r'src="([^"]+)"', _replace, html)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wrap_document(self, body: str) -> str:
        title = _html.escape(self.doc.title or "")
        author = _html.escape(self.doc.author or "")
        sub_title = _html.escape(self.doc.sub_title or "")
        date = _html.escape(str(self.doc._title_date or ""))

        header_parts = []
        if title:
            header_parts.append(f'<h1 class="doc-title">{title}</h1>')
        if sub_title:
            header_parts.append(f'<p class="doc-subtitle">{sub_title}</p>')
        if author:
            header_parts.append(f'<p class="doc-author">{author}</p>')
        if date:
            header_parts.append(f'<p class="doc-date">{date}</p>')

        header_html = ""
        if header_parts:
            header_html = '<div class="header">\n' + "\n".join(header_parts) + '\n</div>\n'

        style_block = ""
        if self._css:
            style_block = f"<style>\n{self._css}\n</style>\n"

        return (
            f'<!DOCTYPE html>\n'
            f'<html lang="{self.lang}">\n'
            f'<head>\n'
            f'<meta charset="utf-8">\n'
            f'<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f'<title>{title}</title>\n'
            f'{style_block}'
            f'</head>\n'
            f'<body>\n'
            f'{header_html}'
            f'<div class="main">\n'
            f'{body}'
            f'</div>\n'
            f'</body>\n'
            f'</html>\n'
        )

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
        return _html.escape(element.text)

    @make.register(doc.Bold)
    def make_bold(self, element: doc.Bold) -> str:
        inner = self._make_elements(element.text)
        return f"<strong>{inner}</strong>"

    @make.register(doc.Emph)
    def make_emph(self, element: doc.Emph) -> str:
        inner = self._make_elements(element.text)
        return f"<em>{inner}</em>"

    @make.register(doc.Code)
    def make_code(self, element: doc.Code) -> str:
        return f'<code class="code-inline">{_html.escape(element.content)}</code>'

    @make.register(doc.SoftBreak)
    def make_soft_break(self, element: doc.SoftBreak) -> str:
        return " "

    @make.register(doc.LineBreak)
    def make_line_break(self, element: doc.LineBreak) -> str:
        return "<br>\n"

    @make.register(doc.HorizontalLine)
    def make_horizontal_line(self, element: doc.HorizontalLine) -> str:
        return "<hr>\n"

    @make.register(doc.PageBreak)
    def make_page_break(self, element: doc.PageBreak) -> str:
        return '<div class="page-break"></div>\n'

    @make.register(doc.Link)
    def make_link(self, element: doc.Link) -> str:
        inner = self._make_elements(element.content)
        target = _html.escape(element.target)
        return f'<a href="{target}">{inner}</a>'

    @make.register(doc.Heading)
    def make_heading(self, element: doc.Heading) -> str:
        level = max(1, min(6, element.level))
        if isinstance(element.content, list):
            text = self._make_elements(element.content)
        else:
            text = _html.escape(element.content)
        return f'<h{level} class="heading-{level}">{text}</h{level}>\n'

    @make.register(doc.Paragraph)
    def make_paragraph(self, element: doc.Paragraph) -> str:
        inner = self._make_elements(element.content)
        return f"<p>{inner}</p>\n"

    @make.register(doc.Block)
    def make_block(self, element: doc.Block) -> str:
        inner = self._make_elements(element.content)
        return f'<p class="block">{inner}</p>\n'

    @make.register(doc.BlockQuote)
    def make_block_quote(self, element: doc.BlockQuote) -> str:
        inner = self._make_elements(element.content)
        return f"<blockquote>\n{inner}</blockquote>\n"

    @make.register(doc.CodeBlock)
    def make_code_block(self, element: doc.CodeBlock) -> str:
        lang_class = f" language-{_html.escape(element.language)}" if element.language else ""
        escaped = _html.escape(element.content)
        return f'<pre><code class="code-block{lang_class}">{escaped}</code></pre>\n'

    @make.register(doc.List)
    def make_list(self, element: doc.List) -> str:
        depth = element.depth if element.depth is not None else 0
        tag = "ol" if element.ordered else "ul"
        inner = self._make_elements(element.content)
        return f'<{tag} class="list-depth-{depth}">\n{inner}</{tag}>\n'

    @make.register(doc.ListItem)
    def make_list_item(self, element: doc.ListItem) -> str:
        inner = self._make_elements(element.content)
        return f"<li>{inner}</li>\n"

    @make.register(doc.Image)
    def make_image(self, element: doc.Image) -> str:
        source = element.source
        is_remote = source.startswith("http://") or source.startswith("https://")
        if self._inline_images and not is_remote:
            src = self._to_data_uri(source)
        else:
            src = _html.escape(source)

        alt = _html.escape(element.title or "")
        align = _html.escape(element.align or "center")
        caption = f"<figcaption>{alt}</figcaption>\n" if alt else ""

        attrs = f'src="{src}" alt="{alt}"'
        if element.width:
            attrs += f' width="{element.width}"'
        if element.height:
            attrs += f' height="{element.height}"'

        return (
            f'<figure class="image image-{align}">\n'
            f'<img {attrs}>\n'
            f'{caption}'
            f'</figure>\n'
        )

    @make.register(doc.Table)
    def make_table(self, element: doc.Table) -> str:
        align = _html.escape(element.align or "center")
        parts = [f'<table class="table table-{align}">\n<thead>\n<tr>\n']

        for col in element.columns:
            heading_align = _html.escape(col.heading_align or "center")
            title = _html.escape(col.title)
            parts.append(
                f'<th style="text-align:{heading_align}">{title}</th>\n'
            )
        parts.append("</tr>\n</thead>\n<tbody>\n")

        for row in element.data:
            parts.append("<tr>\n")
            for col in element.columns:
                cell_align = _html.escape(col.align or "left")
                value = _html.escape(col.formatter(row))
                parts.append(
                    f'<td style="text-align:{cell_align}">{value}</td>\n'
                )
            parts.append("</tr>\n")

        parts.append("</tbody>\n</table>\n")
        return "".join(parts)
