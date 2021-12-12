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
"""
utilities for iteration tasks

=========== =============== =================================================
Aug 2019    Cobus Nel       Added uuid_key function
Jan 2021    Cobus Nel       refactored iter_functions from manipulate.py
Jan 2020    Cobus Nel       added take()
=========== =============== =================================================
"""

from itertools import chain, islice
import fnmatch
import uuid
import base64
from collections_extended import RangeMap
from .stats import quantile_bins
import typing
import sys
import random
from tabulate import tabulate


__all__ = [
    "chunker",
    "glob_list",
    "add_uuid_key",
    "add_key",
    "iter_add_id",
    "iter_drop",
    "iter_take",
    "iter_rename",
    "iter_sample",
    "take",
]


def take(row, *fields):
    """create new dict with just entires specified in fields"""
    return {k: row[k] for k in fields}


def add_uuid_key(iterable, name="uuid"):
    """
    add a uuid key in each dictionary within iterable
    """
    id_fn = uuid.uuid4

    def add_id(row):
        row[name] = str(id_fn())
        return row

    yield from (add_id(i) for i in iterable)


def add_key(iterable, name="uid"):
    """
    Add unique (random) key to each ditionary in iterable

    key is url save
    """
    def add_id(row):
        row[name] = base64.urlsafe_b64encode(uuid.uuid4().bytes).strip(b"=").decode()
        return row

    yield from (add_id(i) for i in iterable)


def chunker(iterable, size=100):
    """
    yield chunks of size `size` from iterator

    Args:
        iterable: iterable from which to chunk data
        size: size of each chunk

    Yields:
        chunks of data
    """
    iterator = iter(iterable)
    for first in iterator:
        yield chain([first], islice(iterator, size - 1))


def glob_list(iterable, glob_list, key=lambda x: x):
    """
    return all items in iterable that match at least
    one of the glob patterns

    Args:
        * iterable:  iterable of objects
        * glob_list: list of glob strings
        * key: key to extract matching string

    Yields:
        * objects that match at least one of the glob strings
    """
    for obj in iterable:
        if any(fnmatch.fnmatch(key(obj), i) for i in glob_list):
            yield obj


def iter_add_id(the_iterator, key="uuid"):
    """
    Add UUID to each row
    :param the_iterator: iterator containing the rows in dictionary format
    :param key: key for uuid
    """
    for row in the_iterator:
        row[key] = str(uuid.uuid4())
        yield row


def iter_add_quantile(the_iterator, value_field, n_quantiles=10, strict=False,
                      field_name="quantile"):
    """
    Add quantile membership to each row

    WARINING: this function will convert input to a list and may
    have heavy memory overhead.

    args:
        * the_iterator: input data
        * value_field: field name for values
        * n_quantiles: number of quantiles
        * strict: generate an error if values overflow to other quantiles
        * field_name: name of new field

    """
    data = list(the_iterator)

    q_map = RangeMap.from_iterable(
        quantile_bins(
            (i[value_field] for i in data),
            n_quantiles,
            strict
        )
    )

    def add_q(row):
        row["quantile"] = q_map[row["amount"]]
        return row

    return map(add_q, data)


def iter_drop(the_iterator, fields):
    """drop specified fields from each row """
    return (
        {k: v for k, v in row.items() if k not in fields}
        for row in the_iterator
    )


def iter_take(the_iterator, fields):
    """yield only specified fields from each row """
    return (
        {k: v for k, v in row.items() if k in fields}
        for row in the_iterator
    )


def iter_sample(the_iterator, p: float = 1,
                k: int = 0) -> typing.Generator[typing.Dict, None, None]:
    """
    generates samples from items in a generator

    Will sample between 1 and k values with probability p.
    When k=None, the function will continue sampling until reaching sys.maxint

    Args:
        - the_iterable: an iterable
        - p: probability of sampling an item
        - k: stop when k is reached (sys.max_size if not defined)
    """
    k = k if k > 0 else (sys.maxsize - 1)
    i = 0
    rand = random.random
    for entry in the_iterator:
        if (rand() <= p):
            yield entry
            i += 1
            if i >= k:
                break


def iter_rename(input, rename_map):
    """rename fields in a dict

    Args:
        - input: input iterator (iter of Dicts)
        - rename_map: rename keys
    """
    for row in input:
        for k, v in rename_map.items():
            row[v] = row.pop(k)
        yield row


def head(input_, n=10):
    """display first n rows as a table"""
    peek_ = []
    for i, row in enumerate(input_):
        if i < n:
            peek_.append(row)

    print(tabulate(peek_, headers="keys"))
