# Copyright (c) 2021 Cobus Nel
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
Simple utilities for console interaction.

Display of dict type records
"""

from typing import Iterable, Dict
from .iteration import first_n, last_n
import tabulate
from .. import TABULATE_NO_FORMAT

__all__ = [
    "head",
    "print_dict",
    "table",
    "tail",
]

FLOAT_FMT = TABULATE_NO_FORMAT


def set_float_format(value):
    """set module float format"""
    global FLOAT_FMT
    FLOAT_FMT = value  # noqa


def print_dict(data: Dict, function=None, float_fmt=None, tablefmt="simple"):
    """Print dictionary in table format

    args:
        - data: the dictionary
        - function: extract function to display value
        - float_fmt: format for float
        - tablefmt: table format
    """
    float_fmt = float_fmt if float_fmt else FLOAT_FMT
    fn = function if function else lambda x: str(x)
    rows = [
        {'key': k, 'value': fn(v)}
        for k, v in data.items()
    ]
    print(
        tabulate.tabulate(
            rows,
            headers="keys",
            floatfmt=FLOAT_FMT,
            tablefmt=tablefmt
        )
    )


def head(data: Iterable[Dict], n: int = 5, float_fmt=None):
    """
    print table of first n rows of list of dicts
    """
    float_fmt = float_fmt if float_fmt else FLOAT_FMT
    print(
        tabulate.tabulate(
            first_n(data, n),
            headers="keys",
            floatfmt=float_fmt
        )
    )


def tail(data: Iterable[Dict], n: int = 5, float_fmt=None):
    """
    print table of first n rows
    """
    float_fmt = float_fmt if float_fmt else FLOAT_FMT
    print(
        tabulate.tabulate(
            last_n(data, n),
            headers="keys",
            floatfmt=float_fmt
        )
    )


def table(data: Iterable[Dict], n: int = 1000, float_fmt=None):
    """
    same as head but with default n of 1000
    """
    float_fmt = float_fmt if float_fmt else FLOAT_FMT
    print(
        tabulate.tabulate(
            first_n(data, n),
            headers="keys",
            floatfmt=float_fmt
        )
    )
