import sys
sys.path.insert(0, "..")  # noqa

import pytest
from dkit.doc2 import document as doc
from dkit.doc2.md_renderer import MarkdownRenderer


def _make_doc(*elements):
    d = doc.Document(title="Test")
    for e in elements:
        d.add_element(e)
    return d


def _render(*elements):
    return MarkdownRenderer(_make_doc(*elements)).render_string()


# ------------------------------------------------------------------
# Inline elements
# ------------------------------------------------------------------

def test_str():
    assert _render(doc.Paragraph([doc.Str("hello")])) == "hello\n\n"


def test_bold():
    md = _render(doc.Paragraph([doc.Bold([doc.Str("strong")])]))
    assert "**strong**" in md


def test_emph():
    md = _render(doc.Paragraph([doc.Emph([doc.Str("italic")])]))
    assert "*italic*" in md


def test_inline_code():
    md = _render(doc.Paragraph([doc.Code("x = 1")]))
    assert "`x = 1`" in md


def test_soft_break_is_space():
    md = _render(doc.Paragraph([doc.Str("a"), doc.SoftBreak(), doc.Str("b")]))
    assert "a b" in md


def test_line_break_is_blank_line():
    md = _render(doc.Paragraph([doc.Str("a"), doc.LineBreak()]))
    assert "\n\n" in md


def test_horizontal_line():
    md = _render(doc.HorizontalLine())
    assert "---" in md


def test_link():
    md = _render(
        doc.Paragraph([doc.Link([doc.Str("text")], "https://example.com")])
    )
    assert "[text](https://example.com)" in md


# ------------------------------------------------------------------
# Headings
# ------------------------------------------------------------------

def test_heading_levels():
    for level in range(1, 7):
        md = _render(doc.Heading(f"H{level}", level=level))
        prefix = "#" * level
        assert md.startswith(f"{prefix} H{level}")


def test_heading_followed_by_blank_line():
    md = _render(doc.Heading("Title", level=1))
    assert md == "# Title\n\n"


# ------------------------------------------------------------------
# Block elements
# ------------------------------------------------------------------

def test_paragraph_ends_with_double_newline():
    md = _render(doc.Paragraph([doc.Str("text")]))
    assert md.endswith("\n\n")


def test_block_quote():
    md = _render(doc.BlockQuote([doc.Paragraph([doc.Str("quoted")])]))
    assert md.startswith("> ")
    assert "quoted" in md


def test_code_block_with_language():
    md = _render(doc.CodeBlock("print('hi')", "python"))
    assert md.startswith("```python\n")
    assert "print('hi')" in md
    assert "```" in md


def test_code_block_no_language():
    md = _render(doc.CodeBlock("x = 1", ""))
    assert md.startswith("```\n")


def test_page_break_is_thematic_break():
    md = _render(doc.PageBreak())
    assert "---" in md


# ------------------------------------------------------------------
# Lists
# ------------------------------------------------------------------

def test_unordered_list():
    lst = doc.List(
        [doc.ListItem([doc.Str("a")]), doc.ListItem([doc.Str("b")])],
        ordered=False,
        depth=0,
    )
    md = _render(lst)
    assert "- a\n" in md
    assert "- b\n" in md


def test_ordered_list():
    lst = doc.List(
        [doc.ListItem([doc.Str("first")]), doc.ListItem([doc.Str("second")])],
        ordered=True,
        depth=0,
    )
    md = _render(lst)
    assert "1. first\n" in md
    assert "2. second\n" in md


def test_nested_list_indented():
    lst = doc.List(
        [doc.ListItem([doc.Str("deep")])],
        ordered=False,
        depth=2,
    )
    md = _render(lst)
    assert "    - deep\n" in md  # 4 spaces = depth 2 * 2


# ------------------------------------------------------------------
# Image
# ------------------------------------------------------------------

def test_image_with_title():
    md = _render(doc.Image("img.png", title="Caption"))
    assert "![Caption](img.png)" in md


def test_image_without_title():
    md = _render(doc.Image("img.png"))
    assert "![](img.png)" in md


# ------------------------------------------------------------------
# Table
# ------------------------------------------------------------------

def _make_table():
    columns = [
        doc.Column(name="name", title="Name", align="left"),
        doc.Column(name="score", title="Score", align="right"),
    ]
    data = [
        {"name": "alpha", "score": 10},
        {"name": "beta", "score": 20},
    ]
    return doc.Table(data=data, columns=columns)


def test_table_header_row():
    md = _render(_make_table())
    assert "| Name | Score |" in md


def test_table_separator_alignment():
    md = _render(_make_table())
    assert "| :--- | ---: |" in md


def test_table_data_rows():
    md = _render(_make_table())
    assert "| alpha | 10 |" in md
    assert "| beta | 20 |" in md


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
