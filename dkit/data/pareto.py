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
Pareto analysis: rank contributors and accumulate their share

Calculation only.  :class:`ParetoAnalysis` sorts pre-aggregated rows by value,
descending, and adds three fields to each::

    cumulative   running total of the value field
    percent      this row's share of the total, 0..100
    cum_percent  running share of the total, 0..100

so that "which few customers are 80% of revenue" is one call::

    from dkit.data.pareto import ParetoAnalysis

    analysis = ParetoAnalysis(rows, value_field="revenue", label_field="customer")
    vital_few = analysis.top_n_percent(80)

This module imports nothing but the standard library: the same numbers feed
the chart (:func:`dkit.plot2.canned.pareto`) and the table
(:func:`dkit.doc2.canned.pareto_table`) without either dragging matplotlib or
the document stack into an analysis script.

Rows must be pre-aggregated -- one row per entity.  Aggregate with
:mod:`dkit.data.aggregation` first if they are not.
"""
from functools import cached_property
from typing import Iterable, Mapping, Union

from ..exceptions import DKitDataException


__all__ = ["ParetoAnalysis"]


class ParetoAnalysis:
    """pareto (80/20) analysis of pre-aggregated rows

    The input rows are never modified: :attr:`calculated_data` works on copies,
    so a caller can run two analyses over the same rows without the second
    seeing the first one's ``cum_percent``.

    args:
        data: iterable of mappings, one row per entity
        value_field: field holding the value to rank and accumulate
        label_field: field naming the entity.  Only needed by
            :meth:`top_n_percent_entities` and by the chart and table recipes.

    raises:
        DKitDataException: if the values total zero, which leaves every share
            undefined rather than merely uninteresting
    """

    def __init__(self, data: Iterable[Mapping], value_field: str,
                 label_field: Union[str, None] = None):
        self.data = list(data)
        self.value_field = value_field
        self.label_field = label_field

    def __repr__(self) -> str:
        return (
            f"ParetoAnalysis({len(self.data)} rows, value_field="
            f"{self.value_field!r}, label_field={self.label_field!r})"
        )

    def __iter__(self):
        return iter(self.calculated_data)

    def __len__(self) -> int:
        return len(self.data)

    @cached_property
    def total(self) -> float:
        """sum of the value field over every row"""
        return sum(row[self.value_field] for row in self.data)

    @cached_property
    def calculated_data(self) -> list[dict]:
        """rows sorted by value, descending, each with the three added fields

        Computed once and cached: every other method reads this list, and the
        sort is the expensive part.
        """
        total = self.total
        if not total:
            raise DKitDataException(
                f"pareto analysis needs a non-zero total for {self.value_field!r}"
            )
        rows = sorted(
            (dict(row) for row in self.data),
            key=lambda row: row[self.value_field],
            reverse=True,
        )
        cumulative = 0.0
        for row in rows:
            value = row[self.value_field]
            cumulative += value
            row["cumulative"] = cumulative
            row["percent"] = 100.0 * value / total
            row["cum_percent"] = 100.0 * cumulative / total
        return rows

    def top_n(self, n: int = 5) -> list[dict]:
        """the ``n`` largest contributors"""
        return self.calculated_data[:n]

    def top_n_percent(self, n: float = 80.0) -> list[dict]:
        """the contributors that make up ``n`` percent of the total

        The row that *crosses* the threshold is included: stopping short of it
        would report a share below ``n``, which is not what "the 80%" means.
        """
        rows = []
        for row in self.calculated_data:
            rows.append(row)
            if row["cum_percent"] >= n:
                break
        return rows

    def top_n_percent_entities(self, n: float = 80.0) -> dict:
        """:meth:`top_n_percent` keyed on ``label_field``

        For asking whether an entity met elsewhere is one of the vital few.
        """
        if self.label_field is None:
            raise DKitDataException(
                "top_n_percent_entities needs a label_field to key on"
            )
        return {row[self.label_field]: row for row in self.top_n_percent(n)}
