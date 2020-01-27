# Copyright (c) 2014 Cobus Nel
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
Infix expression parser.

This parser is designed for fast, repeated evaluation of expressions.

Features include::

    - can include variables provided as a dictionary;
    - easy subclassing to retrieve variables from a host system;
    - ability to add functions on the fly; and
    - maximises parse logic to identify structure errors in the expression at
      parse time.

This class was adapted from the pyparsing example available here:

http://pyparsing.wikispaces.com/file/view/fourFn.py/30154950/fourFn.py

2015 - initial version
2018 - updated for performance
2019 - improved formatting + support for pre-compiled functions such as regex
"""

import math
import operator
from random import (randint, uniform)

from pyparsing import (
    CaselessLiteral,
    Combine,
    Forward,
    Literal,
    Optional,
    QuotedString,
    Word,
    ZeroOrMore,
    alphas,
    delimitedList,
    nums,
    oneOf,
    ParseException,
    originalTextFor,
)

from ..data import helpers
from ..data.containers import ReusableStack
from ..exceptions import CkitParseException
from ..utilities import time_helper
from .helpers import rex_match_closure
from .. import _missing_value_


# def cmp(a, b):
#    """
#    replacement cmp function for python 3
#    """
#    return (a > b) - (a < b)

def replace_na(value, replacement):
    """replace None values with replacement"""
    if value is None:
        return replacement
    else:
        return value


def f1_closure(fn):
    def validate_fn(parser, strg, tokens):
        fname = tokens[0]
        if len(tokens) != 2:
            raise CkitParseException(f"function {fname} require 1 parameter")
        return lambda x: fn(x._internal_eval())
    return validate_fn


def f2_closure(fn):
    def validate_fn(parser, strg, tokens):
        fname = tokens[0]
        if len(tokens) != 3:
            raise CkitParseException(f"function {fname} require 2 parameters")

        def ret_fn(parser):
            par_b = parser._internal_eval()
            par_a = parser._internal_eval()
            return fn(par_a, par_b)
        return ret_fn
    return validate_fn


class InfixParser(object):
    """
    Implement an infix parser for floating point calculations that can be
    used as a standalone class or as basis for more complex parsers.

    All calculations are treated as floating point operations.

    Example use:

    >>> evaluator = InfixParser("${x}*10", variables={"x": 10})
    >>> print(evaluator())
    100.0
    >>> print(evaluator.eval())
    100.0

    The parser can parse arithmetic:

    >>> InfixParser("10*50")()
    500.0

    And comparisons:

    >>> p = InfixParser("${x} == 10 & ${x} < 20")
    >>> p.variables["x"] = 10
    >>> p.eval()
    True

    Arguments:
        expression: string parse expression
    """
    def __init__(self, expression: str = "0", variables: dict = None, functions: dict = None):
        #   epsilon = 1e-12
        self._parse_stack: list = []
        self._evaluation_stack: list = []

        # store names of variables parsed
        self.parsed_variable_names = []

        self._operations_map = {
            "+": operator.add,
            "-": operator.sub,
            "*": operator.mul,
            "/": operator.truediv,
            "^": operator.pow,
            "!=": operator.ne,
            "==": operator.eq,
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
            "&": operator.and_,
            "|": operator.or_,
            }

        self._constants_map = {
            "PI": math.pi,
            "E": math.e,
        }

        self._f1_map = {
            "abs": abs,
            "bool": helpers.to_boolean,
            "capitalize": lambda x: x.capitalize(),
            "cos": math.cos,
            "float": float,
            "lower": lambda x: x.lower(),
            "int": lambda x: int(float(x)),
            "not": lambda x: not x,
            "round": round,
            "sin": math.sin,
            # "sgn": lambda a: abs(a) > epsilon and cmp(a, 0) or 0,
            "sqrt": math.sqrt,
            "str": str,
            "tan": math.tan,
            "title": lambda x: x.title(),
            "trunc": math.trunc,
            "upper": lambda x: x.upper(),
            "from_unixtime": time_helper.from_unixtime,
        }
        if functions is not None:
            self._f1_map.update(functions)

        self._f2_map = {
            "randint": lambda x, y: float(randint(x, y)),
            "uniform": uniform,
            "replace_na": replace_na,
            "strftime": lambda d, f: d.strftime(f)
        }

        # add 1 parameter and 2 parameter functions
        self._functions = {k: f1_closure(v) for k, v in self._f1_map.items()}
        self._functions.update({k: f2_closure(v) for k, v in self._f2_map.items()})
        self._functions["match"] = rex_match_closure()
        self.variables = {} if variables is None else variables
        if (expression is None) or (len(expression) == 0):
            expression = "0"
        self.parse(expression)

    #
    # Private methods
    #
    def __parse_definition(self):
        """
        Initialize pyparsing machinery.
        """
        self._parse_stack = []
        point = Literal(".")

        # constants
        e = CaselessLiteral("E")
        pi = CaselessLiteral("PI")
        constants = pi | e
        float_number = Combine(
            Word("+-" + nums, nums) +
            Optional(point + Optional(Word(nums))) +
            Optional(e + Word("+-" + nums, nums))
        )
        # ident = Word(alphas, alphas+nums+"_$")
        variable = Combine(Literal("${") + Word(alphas + nums + "_ |.#") + Literal("}"))
        plus = Literal("+")
        minus = Literal("-")
        mult = Literal("*")
        div = Literal("/")
        lpar = Literal("(").suppress()
        rpar = Literal(")").suppress()
        # comma = Literal(",").suppress()
        add_op = plus | minus
        multiplication_op = mult | div
        exp_op = Literal("^")
        compare_op = oneOf(["!=", "==", "<", "<=", ">", ">="])
        logical_op = oneOf(["|", "&"])
        expr = Forward()
        quoted_string = QuotedString('"').setParseAction(self.__push_op)
        _function = oneOf(self._functions.keys()) \
            + lpar \
            + delimitedList(originalTextFor(expr) | quoted_string) \
            + rpar
        atom = (
            Optional("-") + (
                float_number.setParseAction(self.__push_value)
                | constants.setParseAction(self.__push_constant)
                | variable.setParseAction(self.__push_variable)
                | _function.setParseAction(self.__push_function)
            ) | (lpar + expr.suppress() + rpar)
        ).setParseAction(self.__push_uminus)

        # by defining exponentiation as "atom [ ^ factor ]..." instead of "atom [ ^ atom ]...",
        # we get right-to-left exponents, instead of left-to-righ
        # that is, 2^3^2 = 2^(3^2), not (2^3)^2.
        factor = Forward()
        factor << atom + ZeroOrMore((exp_op + factor).setParseAction(self.__push_op))
        term = factor + ZeroOrMore((multiplication_op + factor).setParseAction(self.__push_op))
        expr << term + ZeroOrMore((add_op + term).setParseAction(self.__push_op))
        c_expr = (expr | quoted_string) + ZeroOrMore(
            (compare_op + (expr | quoted_string)).setParseAction(self.__push_op)
        )
        l_expr = c_expr + ZeroOrMore((logical_op + c_expr).setParseAction(self.__push_op))
        return l_expr

    @staticmethod
    def __operation_wrapper(cls, op):
        par2 = cls._internal_eval()
        par1 = cls._internal_eval()
        try:
            return op(par1, par2)
        except TypeError as e:
            if par1 is None or par2 is None:
                return 0
            else:
                raise e

    def __push_constant(self, strg, loc, toks):
        """
        add constant to calculation stack
        """
        token = toks[0]
        constant = self._constants_map[token]
        self._parse_stack.append(lambda x: constant)

    def __push_variable(self, strg, loc, toks):
        """
        add variable reference to calculation stack
        """
        token = toks[0]
        if token.startswith("${"):
            self.parsed_variable_names.append(token[2:-1])
            self._parse_stack.append(lambda x: x._get_variable(token[2:-1]))

    def __push_op(self, strg, loc, toks):
        """
        push operator to calculation stack
        """
        token = toks[0]
        if token in self._operations_map:
            operation = self._operations_map[token]
            self._parse_stack.append(lambda x: self.__operation_wrapper(x, operation))
        # else, just add a string value
        else:
            # this is string value
            self._parse_stack.append(lambda x: token)

    def __push_function(self, strg, loc, toks):
        the_function = toks[0]
        fn_compiled = self._functions[the_function](self, strg, toks)
        self._parse_stack.append(fn_compiled)

    def _get_variable(self, x):
        """
        return variable

        This method is overrided by subclasses that manage their own variables.
        """
        return self.variables[x]

    def __push_value(self, strg, loc, toks):
        """
        push value onto stack
        """
        self._parse_stack.append(lambda x: float(toks[0]))

    def __push_uminus(self, strg, loc, toks):
        """
        Add unary minus to stack.
        """
        if toks and toks[0] == '-':
            self._parse_stack.append(lambda x: x._internal_eval() * -1.0)

    def parse(self, str_expression):
        """
        Parse expression and update internal parse stack.

        Args:
            str_expression: expression to parse

        Returns:
            self
        """
        try:
            self.__parse_definition().parseString(str_expression, parseAll=True)
        except ParseException as E:
            raise CkitParseException(E)
        self._evaluation_stack = ReusableStack(self._parse_stack)
        return self

    def _internal_eval(self):
        """
        Returns evaluated expression

        The same as Object.__call__()

        Returns:
            (float) evaluated expression
        """
        return self._evaluation_stack.pop()(self)

    def eval(self):
        """
        Returns evaluated expression

        The same as Object.__call__()

        Returns:
            (float) evaluated expression
        """
        self._evaluation_stack.reset()
        return self._evaluation_stack.pop()(self)

    def eval_vars(self, variables=None):
        """
        set variables and evaluate

        Arguments:
            variables: dictionary of variables

        Returns:
            evaluated function
        """
        # if variables is not None:
        self.variables = variables
        self._evaluation_stack.reset()
        return self._evaluation_stack.pop()(self)

    def __call__(self):
        """
        evaluate expression
        """
        self._evaluation_stack.reset()
        return self._evaluation_stack.pop()(self)


class AbstractFieldParser(InfixParser):
    """
    Parser that can be used as base class for more
    advanced parsers.
    """
    def __init__(self, expression="0", none_value=_missing_value_):
        self.none_value = none_value
        super().__init__(expression, none_value)


class ExpressionParser(AbstractFieldParser):
    """
    Evaluate expression based on dictionary provided

    Refer to ExpressionFilter for usage example.
    """
    def __call__(self, row):
        """
        evaluate expression
        """
        self.variables = row
        self._evaluation_stack.reset()
        return self._evaluation_stack.pop()(self)
