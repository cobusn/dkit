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
from dkit.data.containers import ReusableStack


class TestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.the_list = [1, 2, 3, 4]

    def setUp(self):
        self.stack = ReusableStack(self.the_list)

    def test_pop(self):
        """test removing items"""
        for item in self.the_list[::-1]:
            self.assertEqual(item, self.stack.pop())

    def test_reset(self):
        """test resetting the stack"""
        self.stack.pop()
        self.stack.reset()
        self.assertEqual(self.the_list[-1], self.stack.pop())

    def test_len(self):
        """test len function"""
        self.stack.pop()
        self.assertEqual(len(self.stack), len(self.the_list))

    def test_overflow(self):
        """test for error when pop is called on empty stack"""
        with self.assertRaises(IndexError) as _:
            for i in range(len(self.the_list) + 1):
                self.stack.pop()


if __name__ == '__main__':
    unittest.main()
