# Copyright (c) 2024 Cobus Nel
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

import sys; sys.path.insert(0, "..") # noqa
import unittest
from dkit.utilities.web import hex_to_rgb, rgb_to_hex, CSS_Colors


class TestCase(unittest.TestCase):

    def test_hex_to_rgb(self):
        self.assertEqual(
            (0, 0, 255),
            hex_to_rgb("#0000FF")
        )
        self.assertEqual(
            (0, 0, 255),
            hex_to_rgb("#0000ff")
        )

    def test_raise_invalid(self):
        with self.assertRaises(ValueError):
            hex_to_rgb("#0000FFFF")
        with self.assertRaises(ValueError):
            hex_to_rgb("#0000FFF")

    def test_rgb_to_hex(self):
        self.assertEqual(
            "#0000ff",
            rgb_to_hex(0, 0, 255),
        )

    def test_css_color(self):
        self.assertEqual(
            CSS_Colors.gold,
            "#ffd700"
        )


if __name__ == '__main__':
    unittest.main()
