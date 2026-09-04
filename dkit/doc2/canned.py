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
Canned tables: the presentation half of the analyses in :mod:`dkit.data`

Each recipe here turns an analysis object into a :class:`~.document.Table`,
ready for ``Document.add_element``.  The split is deliberate: the numbers come
from :class:`dkit.data.pareto.ParetoAnalysis` and
:class:`dkit.data.boston.BostonMatrix`, the chart from
:mod:`dkit.plot2.canned`, and the table from here --- so an analysis script
imports none of the document stack, and a report never recomputes what the
chart already showed::

    from dkit.data.boston import BostonMatrix
    from dkit.doc2 import canned
    from dkit.plot2 import canned as charts

    matrix = BostonMatrix(rows, id_field="customer", sequence_field="month",
                          value_field="revenue")
    tables = canned.BostonTables(matrix, description_field="name")

    report.add_element(tables.quadrant(1))
    report.add_element(tables.top_by_value())

The column helpers are public for the same reason the tables are canned: a
report that needs a fifth column builds its own :class:`~.document.Table` from
:meth:`BostonTables.columns` plus that column, rather than waiting for an
argument to be added here.
"""
from string import Template
from typing import Union

from .. import CURRENCY_FORMAT
from ..data.boston import BostonMatrix
from ..data.pareto import ParetoAnalysis
from .document import Column, SparkLine, Table


__all__ = ["BostonTables", "pareto_table"]

#: width of a numeric column, in centimetres
NUMERIC_WIDTH = 2.0


def pareto_table(analysis: ParetoAnalysis, n: Union[int, None] = None,
                 cum_percent: Union[float, None] = None,
                 label_title: Union[str, None] = None, value_title: str = "Value",
                 value_format: str = CURRENCY_FORMAT, align: str = "center") -> Table:
    """the ranked rows of a pareto analysis, with both percentage columns

    args:
        analysis: a :class:`dkit.data.pareto.ParetoAnalysis`
        n: show only the largest ``n`` rows
        cum_percent: show only the rows making up this share of the total.
            Ignored when ``n`` is given.
        label_title: heading for the entity column, defaulting to the field name
        value_title: heading for the value column
        value_format: ``str.format`` spec for the value and cumulative columns
        align: table alignment

    returns:
        a :class:`~.document.Table`
    """
    if n is not None:
        rows = analysis.top_n(n)
    elif cum_percent is not None:
        rows = analysis.top_n_percent(cum_percent)
    else:
        rows = analysis.calculated_data
    percent_format = "{:,.1f}%"
    return Table(
        rows,
        [
            Column(analysis.label_field, label_title or analysis.label_field,
                   width=5, align="left"),
            Column(analysis.value_field, value_title, width=NUMERIC_WIDTH,
                   format_=value_format, align="right"),
            Column("cumulative", "Cumulative", width=NUMERIC_WIDTH,
                   format_=value_format, align="right"),
            Column("percent", "%", width=1.5, format_=percent_format, align="right"),
            Column("cum_percent", "Cum %", width=1.5, format_=percent_format,
                   align="right"),
        ],
        align=align,
    )


class BostonTables:
    """table recipes over a :class:`dkit.data.boston.BostonMatrix`

    Holds the presentation decisions the matrix deliberately does not: headings,
    number formats, column widths, and how many rows a report shows.

    args:
        matrix: the analysis to tabulate
        description_field: field holding a human readable name.  None omits the
            column, which is what a matrix keyed on a name already needs.
        id_title: heading for the identifier column, defaulting to the field name
        description_title: heading for the description column
        value_title: heading for the last value column.  None uses the period
            itself, e.g. ``"2026-06-30"``, which is what that column is.
        value_format: ``str.format`` spec for every currency column
        top_n: rows per table
    """

    def __init__(self, matrix: BostonMatrix, description_field: Union[str, None] = None,
                 id_title: Union[str, None] = None, description_title: str = "Description",
                 value_title: Union[str, None] = None,
                 value_format: str = CURRENCY_FORMAT, top_n: int = 10):
        self.matrix = matrix
        self.description_field = description_field
        self.id_title = id_title or matrix.id_field
        self.description_title = description_title
        self.value_title = value_title
        self.value_format = value_format
        self.top_n = top_n

    def __repr__(self) -> str:
        return f"BostonTables({self.matrix!r}, top_n={self.top_n})"

    #
    # columns
    #
    def col_identifier(self, title: Union[str, None] = None, width: float = 3.0,
                       align: str = "left") -> Column:
        """the entity's identifier"""
        return Column(self.matrix.id_field, title or self.id_title, width=width,
                      align=align)

    def col_description(self, title: Union[str, None] = None, width: float = 6.0,
                        align: str = "left") -> Column:
        """the entity's description"""
        return Column(self.description_field, title or self.description_title,
                      width=width, align=align)

    def col_last_value(self, title: Union[str, None] = None,
                       width: float = NUMERIC_WIDTH) -> Column:
        """the value in the reported period"""
        return Column(
            self.matrix.value_field,
            title or self.value_title or str(self.matrix.last_sequence),
            width=width, format_=self.value_format, align="right",
        )

    def col_median(self, title: str = "Median(${n})",
                   width: float = NUMERIC_WIDTH) -> Column:
        """the windowed median.  ``${n}`` in the title becomes the window size."""
        return Column(self.matrix.alias_median, self._titled(title), width=width,
                      format_=self.value_format, align="right")

    def col_mean(self, title: str = "Mean(${n})",
                 width: float = NUMERIC_WIDTH) -> Column:
        """the windowed mean.  ``${n}`` in the title becomes the window size."""
        return Column(self.matrix.alias_mean, self._titled(title), width=width,
                      format_=self.value_format, align="right")

    def col_growth(self, title: str = "Growth(${n})",
                   width: float = NUMERIC_WIDTH) -> Column:
        """growth per period.  ``${n}`` in the title becomes the window size."""
        return Column(self.matrix.alias_growth, self._titled(title), width=width,
                      format_=self.value_format, align="right")

    def col_sparkline_values(self, title: str = "History",
                             width: float = 2.0) -> SparkLine:
        """a sparkline of the raw value across the trailing window

        Only rendered by :class:`~.latex_renderer.LatexRenderer`: it draws
        with TikZ, so it has no equivalent in the other renderers.
        """
        rows = [r for r in self.matrix.windowed if r[self.matrix.value_field] != 0.0]
        return SparkLine(rows, self.matrix.id_field, self.matrix.id_field,
                         self.matrix.value_field, title=title, width=width)

    def col_sparkline_median(self, title: str = "Median(${n})",
                             width: float = 2.0) -> SparkLine:
        """a sparkline of the windowed median across the trailing window

        ``${n}`` in the title becomes the window size, matching
        :meth:`col_median`, the single-value column this is the trend for.
        """
        rows = [r for r in self.matrix.windowed if r[self.matrix.alias_median] != 0]
        return SparkLine(rows, self.matrix.id_field, self.matrix.id_field,
                         self.matrix.alias_median, title=self._titled(title), width=width)

    def _titled(self, title: str) -> str:
        """substitute the window size into a column title"""
        return Template(title).safe_substitute({"n": self.matrix.window_size})

    def columns(self, with_sparklines: bool = False) -> list[Column]:
        """the standard columns: who, what now, what typically, and which way

        The description column is dropped when there is no description field,
        and the identifier widens to take its place -- unless sparklines are
        also requested, in which case the two extra columns already fill
        that space, and the widen-to-9cm rule would push the row past
        LaTeX's default 17cm text width instead.

        args:
            with_sparklines: insert :meth:`col_sparkline_values` and
                :meth:`col_sparkline_median` before the numeric columns. Off
                by default because they only render through
                :class:`~.latex_renderer.LatexRenderer` -- a table built with
                them looks fine there and loses the columns silently
                elsewhere. Combined with a ``description_field``, the row
                widens past a standard page and needs narrower columns of
                your own -- build the list via the ``col_*`` methods
                directly rather than through this one.
        """
        if self.description_field is None:
            identifier_width = 3.0 if with_sparklines else 9.0
            identifier = [self.col_identifier(width=identifier_width)]
        else:
            identifier = [self.col_identifier(), self.col_description()]
        sparklines = (
            [self.col_sparkline_values(), self.col_sparkline_median()]
            if with_sparklines else []
        )
        return identifier + sparklines + [
            self.col_last_value(), self.col_median(), self.col_growth(),
        ]

    #
    # tables
    #
    def table(self, rows: list, align: str = "left",
              with_sparklines: bool = False) -> Table:
        """the standard columns over rows the caller chose

        The escape hatch for a view this class does not name: every table below
        is this method over one of the matrix's own orderings.
        """
        return Table(list(rows)[:self.top_n], self.columns(with_sparklines), align=align)

    def top_by_value(self, with_sparklines: bool = False) -> Table:
        """the largest entities in the reported period"""
        return self.table(self.matrix.by_value(), with_sparklines=with_sparklines)

    def top_by_growth(self, with_sparklines: bool = False) -> Table:
        """the fastest growing entities"""
        return self.table(self.matrix.by_growth(), with_sparklines=with_sparklines)

    def top_by_decline(self, with_sparklines: bool = False) -> Table:
        """the fastest shrinking entities"""
        return self.table(self.matrix.by_decline(), with_sparklines=with_sparklines)

    def quadrant(self, n: int, with_sparklines: bool = False) -> Table:
        """the entities in quadrant ``n``, most extreme first

        args:
            n: quadrant number, 1 to 4.  See
                :class:`dkit.data.boston.BostonMatrix` for what each means.
            with_sparklines: see :meth:`columns`
        """
        return self.table(self.matrix.quadrant(n), with_sparklines=with_sparklines)
