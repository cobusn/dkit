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
from itertools import chain, islice
import fnmatch
import uuid
import base64

"""
helpers for iterators and generators

=========== =============== =================================================
Aug 2019    Cobus Nel       Added uuid_key function
=========== =============== =================================================
"""

__all__ = [
    "chunker",
    "glob_list",
    "add_uuid_key",
    "add_key"
]


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
