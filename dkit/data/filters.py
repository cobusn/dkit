#
# Copyright (C) 2017  Cobus Nel
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
"""
Classes and functions to assist with filtering data.

=========== =============== =================================================
DATE        NAME            COMMENT
=========== =============== =================================================
2017        Cobus Nel       Initial version
Jan 2018    Cobus Nel       Updated
Jun 2018    Cobus Nel       Updated for revised infix_parser class
=========== =============== =================================================
"""
from ..parsers import infix_parser
import operator as op
import re
from typing import List


__all__ = ["match_filter", "search_filter", "ExpressionFilter","Proxy" ]


def __re_filter(pattern: str, field_list: List = None, flags=0, re_fn="search"):
    """
    helper function to grep dictionary records

    Args:
        * expr: regular expresson string
        * field_list: list of fields, all fields if not defined
        * flags: regex flags

    Returns:
        function that can be used as filter (will return True if match
        is found)
    """
    assert re_fn in ["search", "match"]
    c = re.compile(pattern, flags)
    fn = getattr(c, re_fn)
    if field_list is not None:
        # list provided
        def matcher(row):
            # print(c.search("".join((str(row[i]) for i in field_list))))
            return bool(fn("".join((str(row[i]) for i in field_list))))
    else:
        # search all fields
        def matcher(row):
            # print(c.search("".join(str(i) for i in row.values())))
            return bool(fn("".join(str(i) for i in row.values())))

    return matcher


def search_filter(pattern: str, field_list: List = None, flags=0):
    """
    helper function to grep dictionary records using regular
    expression search (search anywhere in text)

    >>> from dkit.data.filters import ExpressionFilter

    >>> data = [{"name": "John", "age": 33},  {"name": "Susan", "age": 44},]

    Args:
        * expr: regular expresson string
        * field_list: list of fields, all fields if not defined
        * flags: regex flags

    Returns:
        function that can be used as filter (will return True if match
        is found)
    """
    return __re_filter(pattern, field_list, flags, "search")


def match_filter(pattern: str, field_list: List = None, flags=0):
    """
    helper function to grep dictionary records using regular
    expression match (search at beginning of text)

    Args:
        * expr: regular expresson string
        * field_list: list of fields, all fields if not defined
        * flags: regex flags

    Returns:
        function that can be used as filter (will return True if match
        is found.
    """
    return __re_filter(pattern, field_list, flags, "match")


class ExpressionFilter(infix_parser.ExpressionParser):
    """
    Create filter with string expression.

    This class is designed to be used with the python filter function.
    """
    pass


#
# The remainder of the file is dedicated to the Proxy filter
#
class _Comparison(object):
    """
    Abstract class that provide common methods
    """

    def __and__(self, other):
        return _BinaryLogicalComparison(self, other, op.and_)

    def __or__(self, other):
        return _BinaryLogicalComparison(self, other, op.or_)


class _BinaryLogicalComparison(_Comparison):
    """
    Helper class that facilitate binary comparisons
    """
    def __init__(self, left, right, operand):
        self.left = left
        self.right = right
        self.operand = operand

    def __call__(self, data):
        return self.operand(self.left(data), self.right(data))


class _UnaryLogicalComparison(_Comparison):
    """
    Implement unary logical operations
    """
    def __init__(self, instance, operand):
        self.instance = instance
        self.operand = operand

    def __call__(self, data):
        return self.operand(self.instance(data))


class _BinaryTest(_Comparison):
    """
    Helper class that facilitate tests between two objects
    """
    def __init__(self, left, path, comparison):
        self.left = left
        self.path = path
        self.comparison = comparison

    def __call__(self, data):
        value = data
        for node in self.path:
            value = value[node]
        return self.comparison(value, self.left)

    def __invert__(self):
        return _UnaryLogicalComparison(self, op.not_)

    @classmethod
    def isin(self, lhs, rhs):
        return lhs in rhs

    @classmethod
    def match(self, rhs, matcher):
        return bool(matcher.match(rhs))

    @classmethod
    def search(self, rhs, matcher):
        return bool(matcher.search(rhs))

    @classmethod
    def filter(self, rhs, udf):
        return udf(rhs)


class _ExistanceTest(_Comparison):
    """
    Test for existance of key.
    """
    def __init__(self, path):
        self.path = path

    def __invert__(self):
        return _UnaryLogicalComparison(self, op.not_)

    def __call__(self, data):
        value = data
        for node in self.path:
            try:
                value = value[node]
            except Exception:
                return False
        return True


class Proxy(object):
    """
    Helper for creating simple filter classes that can be used by comprehensions
    or the `filter` function.

    The `Proxy()` class provide comprehensive comparison interfaces:

    >>> data = {"name": "John", "age": 33}
    >>> datalist = [{"name": "John", "age": 33},  {"name": "Susan", "age": 44},]
    >>> user = Proxy()

    Filters are defined as:

    >>> test = user.name == 'John'

    The test will simply return true or false:

    >>> test(data)
    True

    Typcial use cases include comprehensions:

    >>> over_30 = (user.age > 30)
    >>> len([i for i in datalist if over_30(i)])
    2

    When combining terms always use brackets:

    >>> test = (user.age > 30) & (user.name == 'John')
    >>> test(data)
    True

    Matching regular expressions (using match or search):

    >>> test = user.name.search(r'^J.*')
    >>> test(data)
    True

    Support nested scope:

    >>> nested = {"name": {"first": "Sophia"}}
    >>> test = user.name.first == "Sophia"
    >>> test(nested)
    True

    Examples:

    >>> (user.age > 10)(data)
    True

    >>> test = ~ (user.age > 10)
    >>> test(data)
    False

    >>> user.age.exists()(data)
    True

    """
    def __init__(self, path=[]):
        self.path = path

    def __getattr__(self, item):
        query = Proxy(self.path)
        query.path = self.path + [item]
        return query

    __getitem__ = __getattr__

    def __eq__(self, other):
        return _BinaryTest(other, self.path, op.eq)

    def __ne__(self, other):
        return _BinaryTest(other, self.path, op.ne)

    def __lt__(self, other):
        return _BinaryTest(other, self.path, op.lt)

    def __gt__(self, other):
        return _BinaryTest(other, self.path, op.gt)

    def __le__(self, other):
        return _BinaryTest(other, self.path, op.le)

    def __ge__(self, other):
        return _BinaryTest(other, self.path, op.ge)

    def isin(self, *argv):
        return _BinaryTest(argv, self.path, _BinaryTest.isin)

    def match(self, expression):
        """implement regex match"""
        matcher = re.compile(expression)
        return _BinaryTest(matcher, self.path, _BinaryTest.match)

    def search(self, expression):
        """implement regex search"""
        matcher = re.compile(expression)
        return _BinaryTest(matcher, self.path, _BinaryTest.search)

    def filter(self, udf):
        """run user defined function to test"""
        return _BinaryTest(udf, self.path, _BinaryTest.filter)

    def exists(self):
        """test existance of attribute"""
        return _ExistanceTest(self.path)
