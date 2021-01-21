# Copyright (c) 2017 Cobus Nel
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

import sys; sys.path.insert(0, "..")  # noqa
import unittest
from dkit.parsers.type_parser import TypeParser


class TestTypeParser(unittest.TestCase):

    def test_valid(self):
        """
        Test type parser parser
        """
        tests = [
            ["String()", {"type": "string"}],
            ["String(computed=True)", {"type": "string", "computed": True}],
            ["String(str_len=None)", {"type": "string", "str_len": None}],
            ["String(str_len=10)", {"type": "string", "str_len": 10}],
            ["String(str_len=10, primary_key=True)",
             {"type": "string", "str_len": 10, "primary_key": True}
             ],
            ["Integer()", {"type": "integer"}],
            ["Integer(nullable=True)", {"type": "integer", "nullable": True}],
            ["Integer(primary_key=True)", {"type": "integer", "primary_key": True}],
            ["Boolean(index=True)", {"type": "boolean", "index": True}],
        ]
        parser = TypeParser()
        for test in tests:
            parsed = parser.parse(test[0])
            self.assertEqual(parsed, test[1])

    def test_invalid_conttent(self):
        with self.assertRaises(ValueError) as _:
            parser = TypeParser()
            parser.parse("Integer(Foo=True)")


if __name__ == '__main__':
    unittest.main()
