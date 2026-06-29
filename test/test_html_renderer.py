import sys
sys.path.insert(0, "..")  # noqa

import os
import struct
import tempfile
import zlib
import pytest
from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer


def _make_doc(*elements):
    d = doc.Document(title="Test Doc", author="Author", sub_title="Sub")
    for e in elements:
        d.add_element(e)
    return d


def _render(*elements, fragment=False):
    return HtmlRenderer(_make_doc(*elements), fragment=fragment).render_string()


# ------------------------------------------------------------------
# Document wrapper
# ------------------------------------------------------------------

def test_full_document_has_doctype():
    html = _render(doc.Paragraph([doc.Str("hello")]))
    assert html.startswith("<!DOCTYPE html>")


def test_full_document_has_title_in_head():
    html = _render(doc.Paragraph([doc.Str("hello")]))
    assert "<title>Test Doc</title>" in html


def test_full_document_header_block():
    html = _render(doc.Paragraph([doc.Str("x")]))
    assert 'class="doc-title"' in html
    assert 'class="doc-author"' in html
    assert 'class="doc-subtitle"' in html


def test_fragment_has_no_doctype():
    html = _render(doc.Paragraph([doc.Str("hello")]), fragment=True)
    assert "<!DOCTYPE" not in html
    assert "<html" not in html


# ------------------------------------------------------------------
# Inline elements
# ------------------------------------------------------------------

def test_str_is_escaped():
    html = _render(doc.Paragraph([doc.Str("<script>alert('x')</script>")]))
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_bold():
    html = _render(doc.Paragraph([doc.Bold([doc.Str("bold")])]))
    assert "<strong>bold</strong>" in html


def test_emph():
    html = _render(doc.Paragraph([doc.Emph([doc.Str("italic")])]))
    assert "<em>italic</em>" in html


def test_inline_code():
    html = _render(doc.Paragraph([doc.Code("x = 1")]))
    assert '<code class="code-inline">x = 1</code>' in html


def test_soft_break_is_space():
    html = _render(doc.Paragraph([doc.Str("a"), doc.SoftBreak(), doc.Str("b")]))
    assert "a b" in html


def test_line_break():
    html = _render(doc.Paragraph([doc.Str("a"), doc.LineBreak()]))
    assert "<br>" in html


def test_horizontal_line():
    html = _render(doc.HorizontalLine())
    assert "<hr>" in html


def test_link():
    html = _render(
        doc.Paragraph([doc.Link([doc.Str("click")], "https://example.com")])
    )
    assert '<a href="https://example.com">click</a>' in html


# ------------------------------------------------------------------
# Block elements
# ------------------------------------------------------------------

def test_heading_levels():
    for level in range(1, 7):
        html = _render(doc.Heading(f"Title {level}", level=level))
        assert f'<h{level} class="heading-{level}">Title {level}</h{level}>' in html


def test_paragraph():
    html = _render(doc.Paragraph([doc.Str("para text")]))
    assert "<p>para text</p>" in html


def test_block_quote():
    html = _render(doc.BlockQuote([doc.Paragraph([doc.Str("quoted")])]))
    assert "<blockquote>" in html
    assert "quoted" in html


def test_code_block_with_language():
    html = _render(doc.CodeBlock("print('hi')", "python"))
    assert 'class="code-block language-python"' in html
    assert "print(&#x27;hi&#x27;)" in html


def test_code_block_no_language():
    html = _render(doc.CodeBlock("x = 1", ""))
    assert 'class="code-block"' in html


def test_page_break():
    html = _render(doc.PageBreak())
    assert 'class="page-break"' in html


# ------------------------------------------------------------------
# Lists
# ------------------------------------------------------------------

def test_unordered_list():
    lst = doc.List(
        [doc.ListItem([doc.Str("a")]), doc.ListItem([doc.Str("b")])],
        ordered=False,
        depth=0,
    )
    html = _render(lst)
    assert '<ul class="list-depth-0">' in html
    assert "<li>a</li>" in html
    assert "<li>b</li>" in html


def test_ordered_list():
    lst = doc.List(
        [doc.ListItem([doc.Str("first")])],
        ordered=True,
        depth=0,
    )
    html = _render(lst)
    assert '<ol class="list-depth-0">' in html


def test_nested_list_depth_class():
    lst = doc.List(
        [doc.ListItem([doc.Str("deep")])],
        ordered=False,
        depth=2,
    )
    html = _render(lst)
    assert 'class="list-depth-2"' in html


# ------------------------------------------------------------------
# Image
# ------------------------------------------------------------------

def test_image_with_title():
    html = _render(doc.Image("img.png", title="A caption", align="center"))
    assert 'class="image image-center"' in html
    assert 'src="img.png"' in html
    assert "<figcaption>A caption</figcaption>" in html


def test_image_without_title_no_figcaption():
    html = _render(doc.Image("img.png", title=None, align="left"))
    assert "<figcaption>" not in html
    assert 'class="image image-left"' in html


def test_image_dimensions():
    html = _render(doc.Image("img.png", width=200, height=100))
    assert 'width="200"' in html
    assert 'height="100"' in html


# ------------------------------------------------------------------
# Table
# ------------------------------------------------------------------

def _make_table():
    columns = [
        doc.Column(name="name", title="Name", align="left", heading_align="center"),
        doc.Column(name="value", title="Value", align="right", heading_align="center"),
    ]
    data = [{"name": "alpha", "value": 1}, {"name": "beta", "value": 2}]
    return doc.Table(data=data, columns=columns, align="center")


def test_table_structure():
    html = _render(_make_table())
    assert '<table class="table table-center">' in html
    assert "<thead>" in html
    assert "<tbody>" in html
    assert "<th" in html
    assert "<td" in html


def test_table_header_titles():
    html = _render(_make_table())
    assert ">Name<" in html
    assert ">Value<" in html


def test_table_data_values():
    html = _render(_make_table())
    assert ">alpha<" in html
    assert ">1<" in html


def test_table_cell_alignment():
    html = _render(_make_table())
    assert 'text-align:right' in html
    assert 'text-align:left' in html


# ------------------------------------------------------------------
# CSS injection
# ------------------------------------------------------------------

def test_css_string_in_head():
    d = doc.Document(title="T")
    renderer = HtmlRenderer(d, css="p { color: red; }")
    html = renderer.render_string()
    assert "<style>" in html
    assert "color: red" in html


def test_css_file_in_head():
    with tempfile.NamedTemporaryFile(suffix=".css", mode="w", delete=False) as f:
        f.write("h1 { font-size: 2em; }")
        css_path = f.name
    try:
        d = doc.Document(title="T")
        renderer = HtmlRenderer(d, css=css_path)
        html = renderer.render_string()
        assert "<style>" in html
        assert "font-size: 2em" in html
    finally:
        os.unlink(css_path)


def test_no_css_no_style_tag():
    d = doc.Document(title="T")
    html = HtmlRenderer(d).render_string()
    assert "<style>" not in html


# ------------------------------------------------------------------
# Image embedding (data URIs)
# ------------------------------------------------------------------

def _make_minimal_png() -> bytes:
    """Return a valid 1x1 white PNG as bytes."""
    def chunk(name, data):
        c = zlib.crc32(name + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + name + data + struct.pack(">I", c)

    signature = b"\x89PNG\r\n\x1a\n"
    ihdr = chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    raw = b"\x00\xFF\xFF\xFF"
    idat = chunk(b"IDAT", zlib.compress(raw))
    iend = chunk(b"IEND", b"")
    return signature + ihdr + idat + iend


def test_inline_images_data_uri():
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(_make_minimal_png())
        img_path = f.name
    try:
        d = doc.Document(title="T")
        d.add_element(doc.Image(img_path, title="banner"))
        html = HtmlRenderer(d, inline_images=True).render_string()
        assert 'src="data:image/png;base64,' in html
    finally:
        os.unlink(img_path)


def test_inline_images_remote_url_unchanged():
    d = doc.Document(title="T")
    url = "https://example.com/banner.png"
    d.add_element(doc.Image(url, title="remote"))
    html = HtmlRenderer(d, inline_images=True).render_string()
    assert f'src="{url}"' in html


def test_inline_images_false_leaves_path():
    d = doc.Document(title="T")
    d.add_element(doc.Image("local/banner.png", title="local"))
    html = HtmlRenderer(d, inline_images=False).render_string()
    assert 'src="local/banner.png"' in html


# ------------------------------------------------------------------
# render_email_string
# ------------------------------------------------------------------

def test_render_email_string_inlines_css():
    d = doc.Document(title="T")
    d.add_element(doc.Paragraph([doc.Str("hello")]))
    renderer = HtmlRenderer(d, css="p { color: navy; }")
    html = renderer.render_email_string()
    # premailer moves rules into style= attributes (normalises to no space)
    assert "color:navy" in html
    # the <style> block should be consumed / empty after inlining
    assert "<style>" not in html or "navy" not in html.split("<style>")[1].split("</style>")[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
