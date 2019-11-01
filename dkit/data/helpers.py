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
# from math import isnan
from .. import _missing_value_


BOOL_MAPPING = {
    1:    True,
    "t": True,
    "true":    True,
}


def sub_nan(value, substitute=0.0):
    """return value while substituting nan values with replacement"""
#   if not isinstance(value, str) and isnan(value):
    if not isinstance(value, str) and value == _missing_value_:
        return substitute
    else:
        return value


def frange(x, y, jump):
    """
    simple floating point version of range
    """
    while x < y:
        yield x
        x += jump


def to_boolean(value):
    """
    convert string to boolean.

    convert string values to boolean.
    :param value: string value
    """
    return BOOL_MAPPING.get(value.lower(), False)


def luhn_hash(number):
    """
    calculate luhn checksum as used to
    validate some credit card numbers
    and ID numbers

    https://en.wikipedia.org/wiki/Luhn_algorithm

    :param number: can be str or numeric
    :returns integer:
    """
    r = [ch for ch in str(number)][::-1]
    a = sum([int(i) for i in r[1::2]])
    b = str(2 * int("".join(r[::2])))
    c = sum([int(i) for i in b])
    return (10 - ((a+c) % 10)) % 10


def validate_luhn_hash(number):
    """
    validate a number with a luhn hash
    """
    num = str(number)
    return luhn_hash(num[:-1]) == int(num[-1])
