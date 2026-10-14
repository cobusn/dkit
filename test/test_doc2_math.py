"""
tests for math notation support in doc2 (dkit/doc2/document.py Math node,
dkit/doc2/md_to_doc.py parsing, dkit/doc2/latex_renderer.py rendering)

See _specs/math.md for the design behind this feature.
"""
import sys
import unittest

sys.path.insert(0, "..")  # noqa

from dkit.doc2 import document as doc
from dkit.doc2.latex_renderer import LatexRenderer
from dkit.doc2.md_to_doc import render_doc_format


class TestMarkdownToMathAST(unittest.TestCase):
    """markdown -> doc.Math parsing"""

    def test_inline_math(self):
        elements = render_doc_format("Inline $a^2+b^2=c^2$ here.")
        paragraph = elements[0]
        math = paragraph.content[1]
        self.assertIsInstance(math, doc.Math)
        self.assertEqual(math.content, "a^2+b^2=c^2")
        self.assertFalse(math.display)

    def test_block_math(self):
        elements = render_doc_format("$$\nx = y\n$$")
        math = elements[0]
        self.assertIsInstance(math, doc.Math)
        self.assertEqual(math.content, "x = y")
        self.assertTrue(math.display)

    def test_display_math_written_inline(self):
        """$$...$$ written on the same line as other text is still
        tokenised as display math by mistune, but nested inside the
        paragraph rather than as a top-level block."""
        elements = render_doc_format("before $$x=y$$ after")
        paragraph = elements[0]
        math = [e for e in paragraph.content if isinstance(e, doc.Math)][0]
        self.assertEqual(math.content, "x=y")
        self.assertTrue(math.display)

    def test_currency_text_is_not_parsed_as_math(self):
        """mistune's math pattern requires the character after a closing
        $ to not be a digit, so a paragraph with two bare dollar amounts
        does not misfire as a math span."""
        elements = render_doc_format("cost between $5 and $10")
        paragraph = elements[0]
        self.assertTrue(all(
            not isinstance(e, doc.Math) for e in paragraph.content
        ))


class TestLatexMathRendering(unittest.TestCase):
    """doc.Math -> native LaTeX delimiters"""

    def _render_str(self, document) -> str:
        renderer = LatexRenderer(document)
        renderer.make_elements(document.elements)
        return str(renderer._tex_doc)

    def test_inline_math_renders_dollar_delimiters(self):
        document = doc.Document("t", "t", "t")
        document.add_element(
            doc.Paragraph([doc.Str("energy "), doc.Math("E=mc^2", display=False)])
        )
        tex = self._render_str(document)
        self.assertIn("$E=mc^2$", tex)

    def test_block_math_renders_double_dollar_delimiters(self):
        document = doc.Document("t", "t", "t")
        document.add_element(doc.Math("x = y", display=True))
        tex = self._render_str(document)
        self.assertIn("$$x = y$$", tex)


if __name__ == "__main__":
    unittest.main()
