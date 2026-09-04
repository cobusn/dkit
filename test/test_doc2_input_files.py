"""
Build test/input_files/input.md through every doc2 renderer.

The v1 counterpart of this (test_markdown_to_pdf.py) only ever exercised
SimpleDocRenderer's default (reportlab) renderer -- nothing exercised docx,
html, or latex against this fixture, which is exactly how three separate
bugs (inline images crashing reportlab/html, an Image.title type mismatch,
a hyperlink wrapped across a source line crashing docx) went unnoticed until
someone ran `dk build doc` by hand. input.md is deliberately the fixture
here: it is the one document exercising nested lists, multi-paragraph
blockquotes, code blocks, native markdown images, the {{ image(...) }} and
{{ include(...) }} jinja helpers, tables, and links -- broad markdown
coverage in one file, rather than one narrow snippet per test.
"""
import shutil
import sys
import unittest
from pathlib import Path

sys.path.insert(0, "..")  # noqa

from dkit.doc2.builder import SimpleDocRenderer


HAS_PDFLATEX = shutil.which("pdflatex") is not None

INPUT_FILE = "input_files/input.md"
OUTPUT_DIR = Path("output")


def _build(renderer: str, suffix: str) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    output = OUTPUT_DIR / f"input_files_{renderer}{suffix}"
    builder = SimpleDocRenderer(
        title="Markdown test document",
        author="Author Name",
        sub_title="From input_files/input.md",
        contact="author@example.com",
        renderer=renderer,
    )
    builder.build_from_files(str(output), INPUT_FILE)
    return output


class TestInputFilesRenderers(unittest.TestCase):
    """each renderer, driven end to end through SimpleDocRenderer"""

    def test_reportlab(self):
        output = _build("reportlab", ".pdf")
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)

    def test_docx(self):
        output = _build("docx", ".docx")
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)

    def test_html(self):
        output = _build("html", ".html")
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)

    @unittest.skipUnless(HAS_PDFLATEX, "pdflatex is not installed")
    def test_latex(self):
        output = _build("latex", ".pdf")
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)
        # the .tex source is a normal, inspectable build output -- check it
        # landed too, since a broken pdflatex run can still leave a stale pdf
        self.assertTrue(output.with_suffix(".tex").exists())


if __name__ == "__main__":
    unittest.main()
