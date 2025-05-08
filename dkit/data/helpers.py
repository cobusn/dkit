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
from .. import NA_VALUE
import pickle
from hashlib import md5
import xxhash
import random
import string

BOOL_MAPPING = {
    1:    True,
    "t": True,
    "true":    True,
}


def scale(values):
    """scale values between 0 and 1

    Args:
        values: iterable

    Yields:
        scaled values
    """
    v = list(values)
    _min = min(v)
    _d = max(v) - _min
    for x in v:
        try:
            yield (x - _min) / _d
        except ZeroDivisionError:
            yield 0


def sub_nan(value, substitute=0.0):
    """return value while substituting nan values with replacement"""
#   if not isinstance(value, str) and isnan(value):
    if not isinstance(value, str) and value == NA_VALUE:
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


def md5_obj_hash(obj) -> str:
    """md5 hash of any object

    hexdigest of md5 hash of pickle of object
    """
    return md5(pickle.dumps(obj)).hexdigest()


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
    return (10 - ((a + c) % 10)) % 10


def validate_luhn_hash(number):
    """
    validate a number with a luhn hash
    """
    num = str(number)
    return luhn_hash(num[:-1]) == int(num[-1])


def get_partition(input_string: str, num_partitions: int = 10, hash=xxhash.xxh32) -> int:
    """Calculates the partition index for a string using xxHash and modulo.

    Args:
        - input_string: The string to partition.
        - num_partitions: The desired number of partitions.

    Returns:
        The partition index (an integer between 0 and num_partitions - 1).
    """
    if num_partitions <= 0:
        raise ValueError("Number of partitions must be positive.")
    hash_value = hash(input_string).intdigest()
    partition_index = hash_value % num_partitions
    return partition_index
