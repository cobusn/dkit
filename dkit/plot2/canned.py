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
Canned analytical charts: the recipe, not just the geom

Where :mod:`dkit.plot2.quick` is one geom in one call, each function here is a
*named analysis* drawn the way that analysis is conventionally read -- a control
chart is a band, a centre line and its breaches; a pareto chart is ranked bars
with a cumulative curve on a second axis::

    from dkit.plot2 import canned

    fig = canned.pareto(rows, value_field="revenue", label_field="customer")
    fig = canned.control_chart(rows, x="date", y="defects")

Every function returns a matplotlib ``Figure``, so a report includes one by
wrapping it with :func:`dkit.doc2.document.wrap_matplotlib` -- there is no plot
grammar to serialise and no decorator to remember.

**Invariant: this module contains no drawing logic.** As in
:mod:`~dkit.plot2.quick`, every function is a signature, argument defaulting,
and one :class:`~dkit.plot2.plot.Plot` expression.  The arithmetic each recipe
needs lives in :mod:`dkit.data`: :class:`dkit.data.pareto.ParetoAnalysis`,
:class:`dkit.data.boston.BostonMatrix` and
:class:`dkit.data.histogram.Histogram` compute the numbers, and the same
numbers feed the tables in :mod:`dkit.doc2.canned`.  Nothing here needs the
document stack, and nothing in those modules needs matplotlib.
"""
import statistics
from typing import Iterable, Mapping, Union

from matplotlib.axes import Axes
from matplotlib.figure import Figure

from ..data.histogram import Histogram
from ..data.pareto import ParetoAnalysis
from . import geom, scale
from .frame import Frame
from .plot import Plot
from .theme import Theme


__all__ = ["control_chart", "histogram", "pareto", "quadrant"]

Rows = Union[Frame, Iterable[Mapping]]


def control_chart(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
                  expected: str = "expected", ucl: str = "ucl", lcl: str = "lcl",
                  label: str = "Observed", title: Union[str, None] = None,
                  xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
                  breach_label: str = "Out of control",
                  limits_label: str = "Control limits",
                  expected_label: str = "Expected",
                  xscale: Union[scale.Scale, None] = None,
                  yscale: Union[scale.Scale, None] = None,
                  where: Union[str, None] = None,
                  theme: Union[Theme, str, None] = None,
                  ax: Union[Axes, None] = None, **options) -> Figure:
    """observations against a centre line and control limits

    The limits are *given*, not computed: which limits are right --- three
    sigma, a moving range, a contractual service level --- is a decision about
    the process, so it belongs upstream of the chart.  Compute them into fields
    (:mod:`dkit.data.window` does moving statistics) and name those fields here.

    Points outside the limits are drawn again in the theme's negative colour, so
    a breach is visible without reading the axis.

    args:
        data: rows, an iterable of mappings, or a Frame
        x: field for the horizontal axis, usually a date.  None uses the row
            index.
        y: field holding the observations
        expected: field holding the centre line
        ucl: field holding the upper control limit
        lcl: field holding the lower control limit
        label: legend entry for the observations
        title: plot title
        xlabel: x axis label
        ylabel: y axis label
        breach_label: legend entry for the out of control points
        limits_label: legend entry for the band between the limits
        expected_label: legend entry for the centre line
        xscale: an explicit x scale.  None infers one from the data.
        yscale: an explicit y scale
        where: filter expression applied to the rows before any layer sees them
        theme: a Theme, the name of one, or None for the default
        ax: draw into this Axes instead of creating a figure
        **options: passed to :class:`~dkit.plot2.plot.Plot`

    returns:
        the Figure drawn on
    """
    frame = Frame(data, where=where)
    return Plot(
        geom.Band(limits_label, x=x, upper=ucl, lower=lcl, color="positive"),
        geom.Line(expected_label, x=x, y=expected, color="neutral", style="--"),
        geom.Line(label, x=x, y=y, marker="o", marker_size=3),
        geom.Scatter(breach_label, x=x, y=y, color="negative", size=45,
                     where=f"${{{y}}} > ${{{ucl}}} | ${{{y}}} < ${{{lcl}}}"),
        x=xscale if xscale is not None else scale.infer(frame.values(x), xlabel),
        y=yscale if yscale is not None else scale.Linear(ylabel),
        title=title, theme=theme, **options,
    ).render(frame, ax=ax)


def histogram(data: Rows, field: str, bins: Union[int, None] = None,
              label: Union[str, None] = None, title: Union[str, None] = None,
              xlabel: Union[str, None] = None, ylabel: str = "Frequency",
              color: Union[str, None] = None, precision: int = 1,
              mean_label: str = "Mean", where: Union[str, None] = None,
              theme: Union[Theme, str, None] = None,
              ax: Union[Axes, None] = None, **options) -> Figure:
    """a frequency distribution with its mean marked

    The reporting variant of :func:`dkit.plot2.quick.hist`: same binning, plus
    a reference line at the mean, because the question asked of a distribution
    in a report is almost always where the bulk sits relative to the average.
    Use ``quick.hist`` for the bars on their own.

    args:
        field: numeric field to bin
        bins: number of bins.  None auto-selects by the Freedman-Diaconis rule.
        label: legend entry for the bars
        precision: digits to round bin boundaries to
        xlabel: x axis label, defaulting to ``field``: the axis *is* the field
        mean_label: legend entry for the reference line

    See :func:`control_chart` for the shared arguments.
    """
    frame = Frame(data, where=where)
    values = frame.values(field)
    return Plot(
        geom.Bar(label, x="midpoint", y="count", color=color, width="width"),
        geom.VLine(statistics.mean(values), label=mean_label),
        x=scale.Linear(xlabel if xlabel is not None else field),
        y=scale.Linear(ylabel),
        title=title, theme=theme, **options,
    ).render(list(Histogram.from_data(values, bins=bins, precision=precision)), ax=ax)


def pareto(data: Union[ParetoAnalysis, Rows], value_field: Union[str, None] = None,
           label_field: Union[str, None] = None, top: Union[int, None] = None,
           label: Union[str, None] = None, title: Union[str, None] = None,
           xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
           cumulative_label: str = "Cumulative",
           rotation: Union[float, None] = 90, xscale: Union[scale.Scale, None] = None,
           yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
           theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
           **options) -> Figure:
    """ranked bars with a cumulative percentage curve on the right axis

    The ranking and the running share are
    :class:`dkit.data.pareto.ParetoAnalysis`'s work.  Pass rows and this builds
    one; pass an analysis you already have and it is reused, which is how the
    chart and :func:`dkit.doc2.canned.pareto_table` are guaranteed to agree.

    args:
        data: rows, or a :class:`~dkit.data.pareto.ParetoAnalysis`
        value_field: field to rank by.  Ignored when ``data`` is an analysis.
        label_field: field naming each entity.  Ignored likewise.
        top: draw only the first ``top`` bars.  The curve still accumulates over
            every row, so the last bar's percentage means what it says.
        label: legend entry for the bars
        cumulative_label: legend entry and right axis label for the curve
        rotation: tick label rotation.  Entity names are long often enough that
            90 degrees is the useful default.

    See :func:`control_chart` for the shared arguments.
    """
    analysis = (
        data if isinstance(data, ParetoAnalysis)
        else ParetoAnalysis(Frame(data, where=where).rows, value_field, label_field)
    )
    return Plot(
        geom.Bar(label, x=analysis.label_field, y=analysis.value_field),
        geom.Line(cumulative_label, x=analysis.label_field, y="cum_percent",
                  axis="right", color="highlight", marker="o", marker_size=3),
        x=xscale if xscale is not None else scale.Categorical(xlabel, rotation=rotation),
        y=yscale if yscale is not None else scale.Linear(ylabel),
        y_right=scale.Percent(cumulative_label, whole=100.0, limits=(0, 105)),
        title=title, theme=theme, **options,
    ).render(analysis.calculated_data[:top], ax=ax)


def quadrant(data: Rows, x: str, y: str, x_center: float = 0.0,
             y_center: Union[float, None] = None, label: Union[str, None] = None,
             title: Union[str, None] = None, xlabel: Union[str, None] = None,
             ylabel: Union[str, None] = None, size: Union[float, str] = 30,
             alpha: float = 0.6, quadrants: tuple = ("Q1", "Q2", "Q3", "Q4"),
             xscale: Union[scale.Scale, None] = None,
             yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
             theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
             **options) -> Figure:
    """a scatter split into four quadrants by two reference lines

    The growth-share picture of :class:`dkit.data.boston.BostonMatrix`: growth
    across, value up, and the corner labels naming the quadrants that class's
    :meth:`~dkit.data.boston.BostonMatrix.quadrant` tabulates::

        canned.quadrant(matrix.classified, x=matrix.alias_growth,
                        y=matrix.alias_median, y_center=matrix.median)

    The cuts are arguments rather than derived from the plotted rows, so a
    filtered chart still divides where the whole population divides.  Only the
    vertical cut has an obvious default, zero growth; ``y_center`` falls back to
    the median of the rows drawn.

    args:
        x: field for the horizontal axis, usually growth
        y: field for the vertical axis, usually the value or its moving median
        x_center: position of the vertical cut
        y_center: position of the horizontal cut.  None uses the median of ``y``.
        label: legend entry for the points
        size: marker area in points squared, or a field name for one size per row
        alpha: marker opacity, low by default because entities overlap
        quadrants: corner labels, anti-clockwise from the top left

    See :func:`control_chart` for the shared arguments.
    """
    frame = Frame(data, where=where)
    upper_left, upper_right, lower_right, lower_left = quadrants
    return Plot(
        geom.Scatter(label, x=x, y=y, size=size, alpha=alpha),
        geom.HLine(
            y_center if y_center is not None else statistics.median(frame.values(y))
        ),
        geom.VLine(x_center),
        geom.Text(upper_left, location="upper left", alpha=0.4),
        geom.Text(upper_right, location="upper right", alpha=0.4),
        geom.Text(lower_right, location="lower right", alpha=0.4),
        geom.Text(lower_left, location="lower left", alpha=0.4),
        x=xscale if xscale is not None else scale.Linear(xlabel),
        y=yscale if yscale is not None else scale.Linear(ylabel),
        title=title, theme=theme, **options,
    ).render(frame, ax=ax)
