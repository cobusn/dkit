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

"""
Classes for simple object data access and serializat
"""

import sys
import unittest
from dataclasses import dataclass, asdict
sys.path.insert(0, "..")
from dkit.data.map_db import ObjectMap, Object


@dataclass
class TestClass(Object):
    a: int
    b: int

    def as_dict(self):
        return asdict(self)


class TestEntityMap(unittest.TestCase):
    """Test the ObjectMap class"""

    @classmethod
    def setUpClass(cls):
        cls.om = ObjectMap(TestClass, None)
        cls.test_instance = TestClass(1, 2)

    def test_add_get(self):
        self.om["one"] = self.test_instance
        instance = self.om["one"]
        self.assertEqual(instance, self.test_instance)

    def test_invalid(self):
        """test raise error on invalid input"""
        with self.assertRaises(ValueError):
            self.om["invalid"] = "invalid data"

    def test_dict(self):
        """to_dict and from_dict methods"""
        self.om["one"] = self.test_instance
        d = self.om.as_dict()
        self.om.from_dict(d)

    def test_from_dict_invalid(self):
        """from_dict with invalid input"""
        with self.assertRaises(TypeError):
            self.om.from_dict({1: {"1": 1}})


if __name__ == '__main__':
    unittest.main()
