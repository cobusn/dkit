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
test routines for dkit/doc/md_to_json

=========== =============== =================================================
Jul 2019    Cobus Nel       Initial version
=========== =============== =================================================
"""
import sys
import pprint

from textwrap import dedent
import mistune
import unittest
sys.path.insert(0, "..")  # noqa
from dkit.doc.json_renderer import JSONRenderer
from dkit.doc import latex_renderer


pp = pprint.PrettyPrinter(indent=4)


class TestMD2Json(unittest.TestCase):
    """Test the Introspect class"""

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.o = JSONRenderer()
        self.md = mistune.Markdown(renderer=JSONRenderer())

    def test_autolink(self):
        """autolink"""
        snippet = """
        website at <http://www.python.org>
        mail to: <mailto:postmaster@python.org>
        """
        o = [{
            'data': [
                {'data': 'website at ', '~>': 'text'},
                {'data': 'http://www.python.org', 'is_email': False, '~>': 'autolink'},
                {'data': '\nmail to: ', '~>': 'text'},
                {'data': 'postmaster@python.org', 'is_email': True, '~>': 'autolink'}
            ],
            '~>': 'paragraph'}
        ]
        markdown = mistune.Markdown(renderer=JSONRenderer())
        c = markdown(dedent(snippet))
        self.assertEqual(c, o)

    def test_block_code(self):
        """block_code"""
        o = self.o.block_code("block", "python")
        c = [{'~>': 'listing', 'data': 'block', 'language': 'python'}]
        self.assertEqual(o, c)

    def test_block_quote(self):
        """block quote"""
        o = self.o.block_quote("quoted text")
        c = [{'~>': 'blockquote', 'data': 'quoted text'}]
        self.assertEqual(o, c)

    def test_codespan(self):
        """codespan"""
        o = self.o.codespan("text")
        c = [{'~>': 'inline', 'data': 'text'}]
        self.assertEqual(o, c)

    def test_double_emphasis(self):
        """double emphasis"""
        c = self.o.double_emphasis('text')
        o = [{'~>': 'bold', 'data': 'text'}]
        self.assertEqual(o, c)

    def test_emphasis(self):
        """emphasis"""
        c = self.o.emphasis('text')
        o = [{'~>': 'emphasis', 'data': 'text'}]
        self.assertEqual(o, c)

    def test_link(self):
        """link"""
        c = self.o.link("link", "title", "text")
        o = [{'~>': 'link', 'data': 'text', 'url': 'link', 'title': 'title'}]
        self.assertEqual(o, c)

    def test_header(self):
        """heading"""
        snippet = """
        # l1
        ## l2
        ### level 3
        """
        markdown = mistune.Markdown(renderer=JSONRenderer())
        c = markdown(dedent(snippet))
        o = [
            {'~>': 'heading', 'data': [{'~>': 'text', 'data': 'l1'}], 'level': 1},
            {'~>': 'heading', 'data': [{'~>': 'text', 'data': 'l2'}], 'level': 2},
            {'~>': 'heading', 'data': [{'~>': 'text', 'data': 'level 3'}], 'level': 3}
        ]
        self.assertEqual(c, o)

    def test_image(self):
        """image"""
        o = self.o.image("test.jpg", "title", "text")
        c = [{'~>': 'image', 'data': 'test.jpg', 'title': 'title', 'align': 'center',
              'text': 'text'}]
        self.assertEqual(o, c)

    def test_list(self):
        """list"""
        snippet = """
        This is a list:

        - one
            * two
        - three
        """
        s = dedent(snippet)
        markdown = mistune.Markdown(renderer=JSONRenderer())
        c = markdown(s)
        o = [{'~>': 'paragraph', 'data': [
            {
                '~>': 'text',
                'data': 'This is a list:'
            }
        ]}, {
            '~>': 'list',
            'data': [
                {'~>': 'entry',
                 'data': [
                     {'~>': 'text', 'data': 'one'},
                     {'~>': 'list', 'data': [
                         {
                             '~>': 'entry',
                             'data': [{'~>': 'text', 'data': 'two'}]
                         }
                     ], 'ordered': False
                     }]},
                {'~>': 'entry', 'data': [{'~>': 'text', 'data': 'three'}]}
            ],
            'ordered': False
        }
        ]

        self.assertEqual(o, c)

    def test_line_break(self):
        """
        line_break
        """
        pass

    def test_paragraph(self):
        """
        paragraph
        26/03/2016 changed 'text\n' to 'text'
        """
        c = [{'~>': 'paragraph', 'data': 'text'}]
        o = self.o.paragraph("text")
        self.assertEqual(o, c)

    def _test_table(self):
        """
        Test table conversion

        26/3/2016 Added
        """
        t = dedent("""size | material     |  color
        ---- | ------------ | ------------
        9    | leather      | brown
        10   | hemp canvas  | natural
        11   | glass        | transparent
        """).strip()
        r = dedent("""\\begin{center}
        \\begin{tabular}{rll}
        \\hline
           size & material    & color       \\\\
        \\hline
              9 & leather     & brown       \\\\
             10 & hemp canvas & natural     \\\\
             11 & glass       & transparent \\\\
        \\hline
        \\end{tabular}
        \\end{center}""").strip()

        # remove all whitespace
        a = "".join(self.md(t).split())
        b = "".join("".join(r.split()))
        self.assertEqual(a, b)


class TestSimple(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open("input_files/input.md") as infile:
            cls.doc = infile.read()
        with open("input_files/reference.tex") as texfile:
            cls.reference = texfile.read()

    def test_parse(self):
        md = mistune.Markdown(renderer=JSONRenderer())
        r = md(self.doc)
        report = latex_renderer.LatexDocRenderer(r)
        # self.assertEqual(self.reference, str(report))
        with open("input_files/simple.tex", "wt") as outfile:
            outfile.write(str(report))


if __name__ == '__main__':
    unittest.main()
