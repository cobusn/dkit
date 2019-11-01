# Copyright (c) 2018 Cobus Nel
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

import unittest
import sys
sys.path.insert(0, "..")
from dkit.parsers.html_parser import HTMLTableParser


class TestHtmlTableParser(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open("output/html_dict_writer_output.html") as infile:
            cls.data = infile.read()

    def test_parse(self):
        p = HTMLTableParser()
        result = list(p.process(self.data))
        self.assertEqual(len(result), 500)

    def test_parse_noheadings(self):
        data = """
        <table>
        <tr><td>10</td><td>50</td></tr>
        </table>
        """
        result = list(HTMLTableParser().process(data))
        self.assertEqual(list(result[0].keys())[0], 0)

if __name__ == '__main__':
    unittest.main()
