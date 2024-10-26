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


import unittest
import sys; sys.path.insert(0, "..")  # noqa
from dkit.utilities.jinja2 import is_template, render, render_strict
from jinja2.exceptions import UndefinedError


class TestJinja2Utilities(unittest.TestCase):

    test_true = """
        {{ is_template }}
    """
    test_false = """ { not at emplate."""
    test_render_str = "{{ variable }}"

    def setUp(self):
        pass

    def test_is_template(self):
        self.assertTrue(
            is_template(self.test_true),
        )
        self.assertFalse(
            is_template(self.test_false)
        )

    def test_render(self):
        ans = render(self.test_render_str, variable="rendered")
        self.assertEqual(
            ans,
            "rendered"
        )

    def test_strict(self):
        with self.assertRaises(UndefinedError):
            render_strict(self.test_render_str, a="1")
        self.assertEqual(
            render(self.test_render_str, variable="rendered"),
            "rendered"
        )


if __name__ == '__main__':
    unittest.main()
