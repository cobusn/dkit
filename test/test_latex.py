#
# Copyright (C) 2016  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
import sys; sys.path.insert(0, "..")  # noqa
import unittest
import common
from dkit.doc import latex
from textwrap import dedent


class TestLatex(common.TestBase):
    """Test the Introspect class"""

    def test_addition(self):
        a = latex.Section("section") + latex.Paragraph("para")
        c = "\\section{section}\npara\n"
        self.assertEqual(str(a), c)

    def test_bold(self):
        self.assertEqual(str(latex.Bold("bold")), r"\textbf{bold}")

    def test_chapter(self):
        self.assertEqual(str(latex.Chapter("chapter")), r"\chapter{chapter}"+"\n")

    def test_comment(self):
        """Latex Comment"""
        self.assertEqual(str(latex.Comment("Comment")), "%% Comment")

    def test_custom_environment(self):
        """CustomEnvironment"""

        class custom(latex.CustomEnvironment):
            pass

        c = '\n\\begin{custom}[language=Python]\ndata\n\\end{custom}\n\n'
        o = custom("data", language='Python')
        self.assertEqual(c, str(o))

    def test_compound_environment(self):
        """Compound Environment"""
        c = '\n\\begin{NotImplemented}\n1\n\\end{NotImplemented}\n'
        o = latex.CompoundEnvironment()
        o.append("1")
        self.assertEqual(c, str(o))

    def _test_document(self):
        check = r"""
        \documentclass[11pt,a4paper]{article}
        \usepackage{longtable}
        \usepackage{graphicx}
        """
        doc = dedent(check).strip() + "\n"
        self.assertEqual(str(latex.Document()), doc)

    def test_emph(self):
        self.assertEqual(str(latex.Emph("emph")), r"\emph{emph}")

    def test_enumerate(self):
        c = ("\n\\begin{enumerate}\n\\item one\n\\item \\textbf{bold}"
             "\\emph{emph}\n\\end{enumerate}\n\n")
        o = latex.Enumerate()
        o.append(latex.Item("one"))
        o.append(latex.Item(latex.Bold("bold") + latex.Emph("emph")))
        self.assertEqual(c, str(o))

    def test_href(self):
        """latex href"""
        c = str(latex.Href("url", "description"))
        self.assertEqual(c, r"\href{url}{description}")

    def test_image(self):
        check1 = r"""
        \begin{center}
        \includegraphics{image}
        \end{center}
        """
        check1 = "\n" + dedent(check1).strip() + "\n"
        c = str(latex.Image("image.jpg", alignment="center"))
        self.assertEqual(c, check1)

    def test_inline(self):
        """verb"""
        c = '\\lstinline!text!'
        o = latex.Inline("text")
        self.assertEqual(str(o), c)

    def test_line_break(self):
        """latex line break
        26/03/2016 Changed check from '(r"\\" + "\n") * 2' to '(r"\\" + "\n")'
        """
        check = (r"\linebreak[2]" + "\n")
        c = str(latex.LineBreak(2))
        self.assertEqual(c, check)

    def test_itemize(self):
        """itemize"""
        c = ('\n\\begin{itemize}\n\\item one\n\\item \\textbf{bold}'
             '\\emph{emph}\n\\end{itemize}\n\n')
        o = latex.Itemize()
        o.append(latex.Item("one"))
        o.append(latex.Item(latex.Bold("bold") + latex.Emph("emph")))
        self.assertEqual(c, str(o))

    def test_listing(self):
        """lstlisting"""
        c = '\n\n\\begin{lstlisting}[language=python]\nText\n\\end{lstlisting}\n'
        o = latex.Listing("Text", language='python')
        self.assertEqual(str(o), c)

    def test_new_page(self):
        """latex new page"""
        self.assertEqual(str(latex.NewPage()), r"\newpage"+"\n")

    def test_paragraph(self):
        """latex paragraph

        25/26/16 Changed test from 'para\n' to 'para'
        """
        self.assertEqual(str(latex.Paragraph("para")), "para\n")

    def test_section(self):
        """latex section"""
        self.assertEqual(str(latex.Section("section")), r"\section{section}"+"\n")

    def test_subsection(self):
        """latex subsection"""
        self.assertEqual(str(latex.SubSection("subsection")), r"\subsection{subsection}"+"\n")

    def test_simple_environment(self):
        c = '\n\\begin{NotImplemented}[language=python,test=True]\nText\n\\end{NotImplemented}\n\n'
        o = latex.SimpleEnvironment("Text", language='python', test=True)
        self.assertEqual(str(o), c)

    def _test_special_characters(self):
        c = "I _am_ sam"
        print(latex.Paragraph(c))

    def test_sweave(self):
        c = '\n\n<<>>==\ndata\n@\n'
        o = latex.Sweave("data")
        self.assertEqual(str(o), c)

    def test_subtitle(self):
        """latex subtitle"""
        self.assertEqual(str(latex.SubTitle("subtitle")), r"\subtitle{subtitle}"+"\n")

    def test_title(self):
        """latex subtitle"""
        self.assertEqual(str(latex.Title("title")), r"\title{title}"+"\n")

    def test_table(self):
        """latex table"""
        check = ('\n\n\\begin{center}\n\\begin{tabular}{rr}\n\\hline\n'
                 ' 1 & 1 \\\\\n\\hline\n\\end{tabular}\n\\end{center}')
        d = [["1", 1]]
        self.assertEqual(str(latex.SimpleTable(d)), check)

    def test_url(self):
        """latex url"""
        c = str(latex.Url("url"))
        self.assertEqual(c, r"\url{url}")

    def test_heading(self):
        """Test headings"""
        c = str(latex.Heading("chapter", 1))
        self.assertEqual(c, r"\chapter{chapter}" + "\n")

    def test_verb(self):
        """verb"""
        c = '\\verb!text!'
        o = latex.Verb("text")
        self.assertEqual(str(o), c)

    def test_verbatim(self):
        """verbatim"""
        check = """\n\\begin{verbatim}\npython.code()\n\\end{verbatim}\n"""
        text = "python.code()"
        o = latex.Verbatim(text)
        self.assertEqual(str(o), check)


if __name__ == '__main__':
    unittest.main()
