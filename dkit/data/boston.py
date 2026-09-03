# Copyright (c) 2026 Cobus Nel
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
Boston (growth-share) matrix over a time series

Calculation only.  :class:`BostonMatrix` takes one row per entity per period
and, over a trailing window, adds::

    ma_<n>     median of the value field over the window
    mean_<n>   mean of the value field over the window
    gr_<n>     gradient (growth per period) of the median
    last_value the value at the end of the window

then classifies each entity in the *last* period into one of four quadrants by
two cuts: the median of every entity's windowed median (high or low value), and
zero growth (rising or falling)::

    Q1  high value, falling      Q2  high value, rising
    Q4  low value, falling       Q3  low value, rising

so the quadrants read anti-clockwise from the top left, which is the numbering
``dkit.doc/canned.py`` used and the one existing reports are written against.

::

    from dkit.data.boston import BostonMatrix

    matrix = BostonMatrix(rows, id_field="customer", sequence_field="month",
                          value_field="revenue", window_size=6)
    for row in matrix.quadrant(1):        # was good, now shrinking: call them
        print(row["customer"], row[matrix.alias_growth])

This module imports nothing outside :mod:`dkit.data` and the standard library:
the chart is :func:`dkit.plot2.canned.quadrant` and the tables are
:mod:`dkit.doc2.canned`, and neither is needed to get at the numbers.
"""
import statistics
from functools import cached_property
from typing import Iterable, Mapping

from . import window as win
from .containers import OrderedSet
from ..exceptions import DKitDataException


__all__ = ["BostonMatrix"]

#: quadrant number to the label added to each row, and used by the chart
QUADRANTS = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}


class BostonMatrix:
    """growth-share analysis of one value tracked over time

    The window fields are named after the window size (``ma_6``, ``gr_6``), so
    two matrices with different windows can be computed over the same rows
    without colliding.  Read those names off :attr:`alias_median`,
    :attr:`alias_mean` and :attr:`alias_growth` rather than formatting them
    again at the call site.

    Every derived collection is cached, and the windowed rows are computed once:
    the window pass is the expensive part of this class.

    args:
        data: iterable of mappings, one row per entity per period
        id_field: field identifying the entity
        sequence_field: field ordering the periods, usually a date
        value_field: field holding the value tracked
        window_size: number of periods in the trailing window
    """

    def __init__(self, data: Iterable[Mapping], id_field: str, sequence_field: str,
                 value_field: str, window_size: int = 6):
        self.data = sorted(data, key=lambda row: row[sequence_field])
        self.id_field = id_field
        self.sequence_field = sequence_field
        self.value_field = value_field
        self.window_size = window_size
        self.alias_median = f"ma_{window_size}"
        self.alias_mean = f"mean_{window_size}"
        self.alias_growth = f"gr_{window_size}"

    def __repr__(self) -> str:
        return (
            f"BostonMatrix({len(self.data)} rows, id_field={self.id_field!r}, "
            f"value_field={self.value_field!r}, window_size={self.window_size})"
        )

    #
    # the window pass
    #
    @cached_property
    def windowed(self) -> list[dict]:
        """every row, with the window fields added

        Two passes, because the gradient is a window over the median: the first
        pass has to have written ``ma_<n>`` before the second can differentiate
        it.
        """
        size = self.window_size
        values = win.MovingWindow(size).partition_by(self.id_field) \
            + win.Median(self.value_field, na=0.0).alias(self.alias_median) \
            + win.Average(self.value_field, na=0.0).alias(self.alias_mean) \
            + win.Last(self.value_field).alias("last_value")

        growth = win.MovingWindow(size).partition_by(self.id_field) \
            + win.Gradient(self.alias_median, na=0).alias(self.alias_growth)

        return list(growth(values(self.data)))

    @cached_property
    def last_sequence(self):
        """the last value of the sequence field, i.e. the period reported on"""
        if not self.data:
            raise DKitDataException("boston matrix needs at least one row")
        return OrderedSet([row[self.sequence_field] for row in self.data]).pop()

    @cached_property
    def last_interval(self) -> list[dict]:
        """the windowed rows of the last period: one row per entity"""
        last = self.last_sequence
        return [row for row in self.windowed if row[self.sequence_field] == last]

    @cached_property
    def median(self) -> float:
        """median of the windowed medians in the last period

        The horizontal cut of the matrix: half the entities are above it.
        """
        return statistics.median(
            row[self.alias_median] for row in self.last_interval
        )

    #
    # ranked views of the last period
    #
    def by_value(self) -> list[dict]:
        """the last period, largest value first"""
        return sorted(
            self.last_interval, key=lambda row: row[self.value_field], reverse=True
        )

    def by_growth(self) -> list[dict]:
        """the growing entities, fastest first"""
        return sorted(
            (row for row in self.last_interval if row[self.alias_growth] >= 0),
            key=lambda row: row[self.alias_growth], reverse=True,
        )

    def by_decline(self) -> list[dict]:
        """the shrinking entities, fastest first"""
        return sorted(
            (row for row in self.last_interval if row[self.alias_growth] < 0),
            key=lambda row: row[self.alias_growth],
        )

    #
    # quadrants
    #
    def quadrant_of(self, row: Mapping) -> int:
        """the quadrant number a windowed row falls in

        The single place the two cuts are expressed, so the chart's reference
        lines and the tables' membership cannot disagree.
        """
        high = row[self.alias_median] > self.median
        rising = row[self.alias_growth] >= 0
        if high:
            return 2 if rising else 1
        return 3 if rising else 4

    def quadrant(self, n: int) -> list[dict]:
        """the last period's rows in quadrant ``n``, tagged and ranked

        Each row gains a ``quadrant`` field (``"Q1"`` .. ``"Q4"``).  Rows are
        ranked by growth, away from the origin: fastest decline first in the
        falling quadrants, fastest growth first in the rising ones, so the top
        of every table is the most extreme case.

        args:
            n: quadrant number, 1 to 4

        raises:
            DKitDataException: if ``n`` is not a quadrant
        """
        if n not in QUADRANTS:
            raise DKitDataException(f"quadrant must be 1, 2, 3 or 4, not {n!r}")
        rows = [row for row in self.classified if row["quadrant"] == QUADRANTS[n]]
        return sorted(
            rows, key=lambda row: row[self.alias_growth], reverse=n in (2, 3)
        )

    @cached_property
    def classified(self) -> list[dict]:
        """the last period with a ``quadrant`` field added to every row

        What the chart plots: one point per entity, and the quadrant available
        for colouring or labelling without classifying twice.
        """
        rows = [dict(row) for row in self.last_interval]
        for row in rows:
            row["quadrant"] = QUADRANTS[self.quadrant_of(row)]
        return rows
