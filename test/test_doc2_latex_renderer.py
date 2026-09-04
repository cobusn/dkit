"""
tests for dkit.doc2.latex_renderer

Three defects fixed together, none previously covered by any test:

* nested lists rendered as Python object reprs instead of nested LaTeX lists
* a code block with no language defaulted to the literal string "text",
  which the listings package does not recognise and rejects with a fatal
  error
* make_table always used SimpleTable (a plain tabulate tabular, no
  page-break support), never LongTable -- a regression from v1, which
  always used LongTable. SparkLine itself could not even be constructed
  (missing dataclass fields), and the LaTeX preamble never defined the
  L/C/R column types or colours LongTable's own header row needs

Every case here is also compiled with a real pdflatex, when available,
because a `.tex` string that merely looks plausible is exactly how these
went unnoticed.
"""
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, "..")  # noqa

from dkit.data.boston import BostonMatrix
from dkit.doc2 import canned, document as doc
from dkit.doc2.latex_renderer import LatexRenderer


HAS_PDFLATEX = shutil.which("pdflatex") is not None


def compile_tex(tex_source: str) -> None:
    """write tex_source to a temp dir and compile it; raise on failure"""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "doc.tex"
        path.write_text(tex_source, encoding="utf-8")
        result = subprocess.run(
            ["pdflatex", "-interaction=nonstopmode", "doc.tex"],
            cwd=tmp, capture_output=True, text=True,
        )
        assert result.returncode == 0, (
            f"pdflatex failed:\n{result.stdout[-4000:]}"
        )


class TestNestedLists(unittest.TestCase):

    def _render_str(self, document) -> str:
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        return str(renderer._tex_doc)

    def test_nested_list_renders_as_latex_not_repr(self):
        markdown = "* Item 1\n* Item 2\n    - sub 1\n    - sub 2\n"
        document = doc.Document("t", "t", "t")
        document.add_template(markdown)
        tex = self._render_str(document)
        self.assertIn(r"\item sub 1", tex)
        self.assertIn(r"\item sub 2", tex)
        self.assertNotIn("ListItem", tex)

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_nested_list_compiles(self):
        markdown = "* Item 1\n* Item 2\n    - sub 1\n    - sub 2\n"
        document = doc.Document("t", "t", "t")
        document.add_template(markdown)
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        compile_tex(renderer._tex_doc.preamble() + "\\begin{document}\n"
                    + "".join(str(o) for o in renderer._tex_doc.content)
                    + "\n\\end{document}\n")


class TestMultiParagraphBlockquote(unittest.TestCase):
    """a blockquote with a blank line inside becomes more than one
    Paragraph in its content, the same shape a nested list has -- and the
    same bug when missed: a bare Paragraph falls through to its Python
    repr instead of its own text."""

    def _render_str(self, document) -> str:
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        return str(renderer._tex_doc)

    def test_second_paragraph_renders_as_latex_not_repr(self):
        markdown = "> First paragraph.\n>\n> Second paragraph.\n"
        document = doc.Document("t", "t", "t")
        document.add_template(markdown)
        tex = self._render_str(document)
        self.assertIn("First paragraph.", tex)
        self.assertIn("Second paragraph.", tex)
        self.assertNotIn("Paragraph(", tex)

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_multi_paragraph_blockquote_compiles(self):
        markdown = "> First paragraph.\n>\n> Second paragraph.\n"
        document = doc.Document("t", "t", "t")
        document.add_template(markdown)
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        compile_tex(str(renderer._tex_doc))


class TestCodeBlockLanguage(unittest.TestCase):

    def test_no_language_omits_the_option(self):
        document = doc.Document("t", "t", "t")
        document.add_element(doc.CodeBlock("print(1)", language=None))
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        tex = str(renderer._tex_doc)
        self.assertIn(r"\begin{lstlisting}", tex)
        self.assertNotIn("language=", tex)

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_no_language_compiles(self):
        document = doc.Document("t", "t", "t")
        document.add_element(doc.CodeBlock("print(1)", language=None))
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        compile_tex(renderer._tex_doc.preamble() + "\\begin{document}\n"
                    + "".join(str(o) for o in renderer._tex_doc.content)
                    + "\n\\end{document}\n")


class TestSparklines(unittest.TestCase):

    def _matrix(self) -> BostonMatrix:
        rows = [
            {"id": "a", "year": year, "value": value}
            for year, value in enumerate([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], start=2020)
        ] + [
            {"id": "b", "year": year, "value": value}
            for year, value in enumerate([6.0, 5.0, 4.0, 3.0, 2.0, 1.0], start=2020)
        ]
        return BostonMatrix(rows, id_field="id", sequence_field="year",
                            value_field="value", window_size=3)

    def test_sparkline_dataclass_constructs(self):
        s = doc.SparkLine([{"x": 1}], "a", "a", "x", title="History")
        self.assertEqual(s.title, "History")
        self.assertEqual(s.align, "center")

    def test_boston_tables_sparkline_columns(self):
        bt = canned.BostonTables(self._matrix())
        columns = bt.columns(with_sparklines=True)
        sparklines = [c for c in columns if isinstance(c, doc.SparkLine)]
        self.assertEqual(len(sparklines), 2)

    def test_sparkline_table_renders_tikz(self):
        bt = canned.BostonTables(self._matrix())
        table = bt.top_by_value(with_sparklines=True)
        document = doc.Document("t", "t", "t")
        document.add_element(table)
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        tex = str(renderer._tex_doc)
        self.assertIn(r"\begin{tikzpicture}", tex)
        self.assertIn(r"\begin{longtable}", tex)

    def test_table_without_sparklines_also_uses_longtable(self):
        """every table goes through LongTable, matching v1's make_table --
        SimpleTable's plain tabular has no page-break support, so a table
        without a sparkline column should not be styled differently."""
        bt = canned.BostonTables(self._matrix())
        table = bt.top_by_value()
        document = doc.Document("t", "t", "t")
        document.add_element(table)
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        tex = str(renderer._tex_doc)
        self.assertIn(r"\begin{longtable}", tex)

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_sparkline_table_compiles(self):
        bt = canned.BostonTables(self._matrix())
        table = bt.top_by_value(with_sparklines=True)
        document = doc.Document("t", "t", "t")
        document.add_element(table)
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        compile_tex(renderer._tex_doc.preamble() + "\\begin{document}\n"
                    + "".join(str(o) for o in renderer._tex_doc.content)
                    + "\n\\end{document}\n")


class TestTableColors(unittest.TestCase):
    """LongTable's header row needs tableheader/tabletextcolor from
    somewhere. \\definecolor does not error on a second definition the way
    \\newcolumntype does -- it silently overwrites -- so defining a default
    unconditionally would clobber a custom class's own brand colours (as
    ntt-article.cls's blue/white header was, before this was fixed) with no
    warning at all."""

    def test_builtin_class_gets_the_default_colors(self):
        renderer = LatexRenderer(doc.Document("t"))
        tex = str(renderer._tex_doc)
        self.assertIn(r"\definecolor{tableheader}", tex)
        self.assertIn(r"\definecolor{tabletextcolor}", tex)

    def test_custom_class_is_not_given_a_conflicting_default(self):
        renderer = LatexRenderer(doc.Document("t"), doc_type="ntt-article")
        tex = str(renderer._tex_doc)
        self.assertNotIn(r"\definecolor{tableheader}", tex)
        self.assertNotIn(r"\definecolor{tabletextcolor}", tex)


class TestTitlePage(unittest.TestCase):
    """\\maketitle is the one hook every class -- built in or custom -- is
    expected to give its own meaning to, rather than LatexRenderer needing
    to know a class-specific command name (e.g. the ntt-article.cls in
    latex_temp/ used to require a bespoke \\isTitlePage instead)."""

    def _document(self, **kwargs):
        document = doc.Document(**kwargs)
        document.add_element(doc.Paragraph([doc.Str("body")]))
        return document

    def test_maketitle_only_when_title_set(self):
        with_title = LatexRenderer(self._document(title="T"))
        without_title = LatexRenderer(self._document())
        self.assertIn(r"\maketitle", str(with_title._tex_doc))
        self.assertNotIn(r"\maketitle", str(without_title._tex_doc))

    def test_standard_fields_reach_the_preamble(self):
        renderer = LatexRenderer(self._document(
            title="Report Title", author="Jane Doe", title_date="2026-01-01",
        ))
        tex = str(renderer._tex_doc)
        self.assertIn(r"\title{Report Title}", tex)
        self.assertIn(r"\author{Jane Doe}", tex)
        self.assertIn(r"\date{2026-01-01}", tex)

    def test_extra_fields_use_provide_then_renew(self):
        """\\providecommand alone would lose to a class that already
        defines the name with its own default -- \\renewcommand right
        after it is what makes the document's value win either way."""
        renderer = LatexRenderer(self._document(
            title="T", sub_title="Sub", contact="a@b.com", version="1.2",
        ))
        tex = str(renderer._tex_doc)
        for name, value in (("subtitle", "Sub"), ("contact", "a@b.com"),
                            ("version", "1.2")):
            self.assertIn(f"\\providecommand{{\\{name}}}{{}}", tex)
            self.assertIn(f"\\renewcommand{{\\{name}}}{{{value}}}", tex)

    def test_unset_extra_fields_are_omitted(self):
        renderer = LatexRenderer(self._document(title="T"))
        tex = str(renderer._tex_doc)
        for name in ("subtitle", "contact", "version"):
            self.assertNotIn(f"\\{name}", tex)

    def test_geometry_skipped_for_a_custom_class(self):
        """a custom class is expected to set its own page geometry (as
        ntt-article.cls does); loading geometry a second time with
        different options is a hard error, not a harmless override."""
        builtin = LatexRenderer(self._document(title="T"))
        custom = LatexRenderer(self._document(title="T"), doc_type="myreport")
        self.assertIn("geometry", str(builtin._tex_doc))
        self.assertNotIn("geometry", str(custom._tex_doc))

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_title_page_compiles(self):
        renderer = LatexRenderer(self._document(
            title="Report Title", sub_title="Sub", author="Jane Doe",
            contact="a@b.com", version="1.2",
        ))
        compile_tex(str(renderer._tex_doc))


if __name__ == "__main__":
    unittest.main()
