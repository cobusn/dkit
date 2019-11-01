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
sys.path.insert(0, "..") # noqa
import dkit
from dkit.utilities.introspection import (
    ClassDocumenter, ModuleDocumenter, RootDocumenter, FunctionDocumenter
)


from dkit.data import manipulate


class TestClass(object):
    """
    Docstring
    """

    test = 10

    @property
    def a_property(self):
        return 10

    def a_method(self):
        pass

    def a_function(self, value: int = 10) -> int:
        """
        docstring
        """
        return value * value

    def b_function(self):
        """
        not annotated
        """
        return None


class TestIntrospection(unittest.TestCase):

    def test_class_uninstantiated(self):
        t = ClassDocumenter(TestClass, show_dunders=False)
        self.assertEqual(len(t.methods), 0)
        self.assertEqual(len(t.routines), 3)
        self.assertEqual(len(t.properties), 1)
        self.assertGreater(len(str(t)), 10)
        t = ClassDocumenter(TestClass, show_dunders=True)
        self.assertGreater(len(t.methods), 1)
        self.assertGreater(len(t.routines), 1)
        self.assertEqual(len(t.properties), 1)
        self.assertGreater(len(str(t)), 10)

    def test_class_instantiated(self):
        t = ClassDocumenter(TestClass())
        # self.assertEqual(len(t.functions), 0)
        self.assertEqual(len(t.methods), 3)
        self.assertEqual(len(t.routines), 3)
        self.assertEqual(len(t.properties), 0)

    def test_module(self):
        t = ModuleDocumenter(manipulate)
        self.assertGreater(len(t.classes), 1)
        self.assertGreater(len(t.routines), 1)
        self.assertGreater(len(t.doc), 1)
        self.assertGreater(len(str(t)), 10)

    def test_package(self):
        t = ModuleDocumenter(dkit)
        self.assertGreater(len(t.packages), 1)
        self.assertGreater(len(t.modules), 1)
        self.assertGreater(len(str(t)), 10)

    def test_root_documentor(self):
        t = RootDocumenter()
        self.assertGreater(len(t.packages), 1)
        self.assertGreater(len(str(t)), 10)

    def test_function_documentor(self):
        t = FunctionDocumenter(TestClass.a_function)
        self.assertEqual(len(t.parameters), 2)
        self.assertEqual(len(t.annotated_parameters), 2)
        self.assertEqual(t.returns, "<class 'int'>")
        self.assertGreater(len(str(t)), 10)
        t = FunctionDocumenter(TestClass.b_function)
        self.assertEqual(len(t.parameters), 1)
        self.assertEqual(len(t.annotated_parameters), 1)
        self.assertEqual(t.returns, None)
        self.assertGreater(len(str(t)), 10)


if __name__ == "__main__":
    unittest.main()
