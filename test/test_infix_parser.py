# Copyright (c) 2015 Cobus Nel
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

# 2017 Update for python 3
# 2018 Performance updates

import unittest
from math import sin, cos, tan, trunc, sqrt, pi, e
import sys
sys.path.insert(0, "..")
from dkit.parsers.infix_parser import InfixParser, f2_closure, f1_closure

from dkit.exceptions import CkitParseException


class TestInfixParser(unittest.TestCase):
    """Test InfixParsers class"""

    def setUp(self):
        super(TestInfixParser, self).setUp()

    def test_operators(self):
        """operators"""
        operator_tests = {
            "addition": ("10 + 10.5", 10 + 10.5),
            "subtract": ("10 -5.5", 10-5.5),
            "multiply": ("10*10.0", 10*10.0),
            "divide": ("123/2", 123/2),
            "power1": ("15.2^3", 15.2**3),
            "power2": ("15.2^-3", 15.2**-3.0),
            "power3": ("3^2^3", 3**2**3),
            "unary min1": ("--9", 9),
            "unary min2": ("-5*5", -5 * 5),
        }
        for test, spec in operator_tests.items():
            result = InfixParser(spec[0])()
            check = spec[1]
            self.assertEqual(result, check)

    def test_comparisons(self):
        """Test comparisons"""
        comparisons = [
            ["4>=4", True],
            ["4<5", True],
            ["4>2", True],
            ["sin(5^2) == sin(5^2)", True],
            ["sin(5^2)==sin( 5^2 )", True],
            ["4>=4", True],
            ["4<=4", True],
            ["4<=4.001", True],
            ["5^2<(5^3)", True],
            ["5^sin(2^3)==(2+3)^sin(2^3)", True],
            ["pi==PI", True],
            ["sin(pi)==sin(pi)", True],
            ["1==1", True],
            ["1<=1", True],
            ["1>=1", True],
            ["1<1", False],
            ["1>1", False],
            ["1==1 & 2==2 & 3==3", True],
            ["1==1 & 2==2 | 3==3", True],
            ['"a"=="a"', True],
        ]
        for check in comparisons:
            output = InfixParser(check[0])()
            self.assertEqual(output, check[1], "Test: '%s'=%s failed." % (check[0], str(check[1])))

    def test_parenthesis(self):
        """Test parenthesis"""
        examples = [
            ["(10+5)", (10+5)],
            ["(10+5) * (12*5)", (10+5) * (12*5)],
            ["(10+5) * sin(12*5)", (10+5) * sin(12*5)],
        ]
        for example in examples:
            self.assertEqual(InfixParser(example[0])(), example[1])

    def test_regex(self):
        """Test regular expression"""
        examples = [
            ['match("yes","y.+")', True],
            ['match("no","y.+")', False],
            ['match("ooo","^o+$")', True],
        ]
        for example in examples:
            self.assertEqual(InfixParser(example[0])(), example[1])

    def test_regex_raise(self):
        """test raise exception with incorrect parameters"""
        examples = [
            ['match("yes","","")']
        ]
        for example in examples:
            with self.assertRaises(CkitParseException) as _:
                InfixParser(example[0])

    def test_operator_precedence(self):
        """Test operator precedecne"""
        examples = [
            ["10*5/3", 10*5/3],
            ["(0.30+0.2)/sin(10)-cos(10)", (0.30+0.2)/sin(10)-cos(10)]
        ]
        for example in examples:
            self.assertEqual(InfixParser(example[0])(), example[1])

    def test_structure_errors(self):
        """
        Test that exceptions is raised with incorrectly structured
        equations.
        """
        examples = [
            "/10",
            "58 3)",
        ]
        for example in examples:
            with self.assertRaises(CkitParseException):
                InfixParser(example)
                print(example)

    def test_keywords(self):
        """Test keywords"""
        keywords = [
            ["e", e],
            ["pi", pi],
            ["-PI", -pi],
        ]
        for example in keywords:
            self.assertEqual(InfixParser(example[0])(), example[1])

    #
    # Functions
    #
    def test_function_errors(self):
        """Test that exceptions is raised with incorrect function parameters"""
        bad_stuff = [
            "sin(2,2)",
            "sin()"
            "nosuch(1)"
        ]
        for example in bad_stuff:
            with self.assertRaises(CkitParseException):
                InfixParser(example)
                print(example)

    def test_functions(self):
        """Test parse sin function."""
        functions = [
            ["abs(-10*2)", abs(-10*2)],
            ["abs(-10.5)", abs(-10.5)],
            ["cos(10)", cos(10)],
            ["sin(10)", sin(10)],
            ["sqrt(10)", sqrt(10)],
            ["tan(10)", tan(10)],
            ["trunc(10.5)", trunc(10.0)]
        ]
        for example in functions:
            self.assertEqual(InfixParser(example[0])(), example[1])

    # Two parameter functions.
    def test_fn2_raise(self):
        """Test that error is raised if incorrect number of parameters in expression"""
        parser = InfixParser()
        with self.assertRaises(CkitParseException):
            parser.parse("randint(1)")
            parser.parse("randint(1,1,1)")
            parser.parse("nosuch(1,1)")

    def test_fn2_randint(self):
        """Test parse randint function"""
        output = InfixParser("randint(2,10)").eval()
        check = [float(i) for i in range(2, 11)]
        self.assertIn(output, check)

    def test_fn2_uniform(self):
        """Test uniform function"""
        output = InfixParser("uniform(1,10)").eval()
        self.assertGreaterEqual(output, 1)
        self.assertLessEqual(output, 10)

    #
    # Test add ability to expand function list on the fly..
    #
    def test_add_function(self):
        """Test ability to add arbitrary functions during runtime."""
        def sinsin(x):
            return sin(sin(x))

        output = InfixParser(
            "sinsin(10)",
            functions={"sinsin": sinsin}
        )()
        self.assertEqual(output, sin(sin(10)))

    def test_add_2par_functions(self):
        """Test ability to add two parameter functions during runtime."""
        def sincos(x, y):
            return sin(x) * cos(y)
        parser = InfixParser()
        parser._functions["sincos"] = f2_closure(sincos)
        # parser.functions_2par["sincos"] = sincos
        output = parser.parse("sincos(10,5)").eval()
        self.assertEqual(output, sin(10) * cos(5))

    def test_string_function(self):
        def lower(x):
            return x.lower()

        parser = InfixParser()
        parser._functions["lower"] = f1_closure(lower)
        output = parser.parse('lower("UPPER")').eval()
        self.assertEqual(output, "upper")

    #
    # Variables
    #
    def test_variable(self):
        """Test parse sin function."""
        parser = InfixParser()
        parser.variables["X"] = 1984
        parser.variables["Y"] = 2024
        parser.variables["Y_"] = 2024
        parser.variables["Y_1"] = 2024

        output = parser.parse("${X}")()
        self.assertEqual(output, 1984)
        output = parser.parse("${Y_1}")()
        self.assertEqual(output, 2024)
        output = parser.parse("${Y}+${X}")()
        self.assertEqual(output, 1984.0 + 2024.0)

    def test_variable_formula(self):
        """Test variables used in formula."""
        parser = InfixParser("0.2*sin(${X})")
        parser.variables["X"] = 0.3
        output = parser.eval()
        self.assertEqual(output, 0.2*sin(0.3))
        output = parser.parse("5^${X}")()
        self.assertEqual(output, 5.0**0.3)

    def test_variable_string_function(self):
        """Test variables used in formula."""
        parser = InfixParser("upper(${X})")
        parser.variables["X"] = "lower"
        output = parser.eval()
        self.assertEqual(output, "LOWER")

    def test_eval_vars(self):
        """eval_vars"""
        parser = InfixParser("sin(${x})")
        values = [float(i) for i in range(1, 10)]
        for i in values:
            y = parser.eval_vars({"x": i})
            self.assertEqual(y, sin(i))


if __name__ == '__main__':
    unittest.main()
