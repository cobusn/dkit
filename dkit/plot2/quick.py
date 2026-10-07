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
One-call plots: the common case in a single function

Each function here takes rows and field names and returns a Figure::

    from dkit.plot2 import quick

    fig = quick.bar(rows, x="month", y="revenue", title="2026 Sales")
    fig = quick.line(rows, x="date", y="value", ylabel="Units")

``x`` and ``y`` are always *field names*.  The scale for each axis is inferred
from the data — a ``date`` field gets a real time axis, a string field a
categorical one, anything else a linear one (see
:func:`dkit.plot2.scale.infer`) — and ``xscale=`` / ``yscale=`` override the
inference with a scale of your own::

    fig = quick.scatter(rows, x="size", y="cost", xscale=scale.Log())

**Invariant: this module contains no drawing logic.** Every function here is a
signature, argument defaulting, and one :class:`~dkit.plot2.plot.Plot`
expression. If a recipe needs an ``if`` that is not defaulting, that logic
belongs in a geom, so that both surfaces stay one implementation. The same rule
governs statistics: :func:`hist` bins with :mod:`dkit.data.histogram` rather
than counting anything itself.

Anything these functions cannot express is a reason to use ``Plot`` directly,
not a reason to add a keyword argument. Once the *structure* of the layers is
the caller's choice — a target line, an overlay, a twin axis — keyword
arguments stop scaling and ``Plot`` is the honest surface.
"""
from typing import Iterable, Mapping, Union

from matplotlib.axes import Axes
from matplotlib.figure import Figure

from ..data.histogram import Histogram
from . import geom, scale
from .frame import Frame
from .plot import Plot
from .theme import Theme


__all__ = [
    "area", "bar", "calendar_heatmap", "heatmap", "hist", "line",
    "plot_histogram", "scatter", "slope", "stem", "treemap"
]

Rows = Union[Frame, Iterable[Mapping]]


def _render(layer, frame: Frame, x: Union[scale.Scale, None],
            y: Union[scale.Scale, None], xlabel: Union[str, None],
            ylabel: Union[str, None], ax: Union[Axes, None], **options) -> Figure:
    """default both scales from the data, then hand off to tier D

    The one place scale inference happens, so every quick function defaults its
    axes the same way.
    """
    return Plot(
        layer,
        x=x if x is not None else scale.infer(frame.values(layer.x), xlabel),
        y=y if y is not None else scale.infer(frame.values(layer.y), ylabel),
        **options,
    ).render(frame, ax=ax)


def line(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
         label: Union[str, None] = None, title: Union[str, None] = None,
         xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
         color: Union[str, None] = None, marker: Union[str, None] = None,
         style: Union[str, None] = None, xscale: Union[scale.Scale, None] = None,
         yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
         theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
         **options) -> Figure:
    """a single line

    args:
        data: rows, an iterable of mappings, or a Frame
        x: x field name.  None uses the row index.
        y: y field name
        label: legend entry.  None draws no legend.
        title: plot title
        xlabel: x axis label.  None draws none: field names do not make good
            axis labels, so this is not defaulted to ``x``.
        ylabel: y axis label
        color: a semantic name, a matplotlib colour, or None for the first
            colour in the theme's cycle
        marker: matplotlib marker, e.g. ``"o"``
        style: line style, e.g. ``"--"``
        xscale: an explicit x scale, overriding inference
        yscale: an explicit y scale
        where: filter expression applied to the rows
        theme: a Theme, the name of one, or None for the default
        ax: draw into this Axes instead of creating a figure
        **options: passed to :class:`~dkit.plot2.plot.Plot`

    returns:
        the Figure drawn on
    """
    frame = Frame(data, where=where)
    return _render(
        geom.Line(label, x=x, y=y, color=color, marker=marker, style=style),
        frame, xscale, yscale, xlabel, ylabel, ax, title=title, theme=theme,
        **options,
    )


def bar(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
        label: Union[str, None] = None, title: Union[str, None] = None,
        xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
        color: Union[str, None] = None, horizontal: bool = False,
        width: Union[float, str] = 0.8, xscale: Union[scale.Scale, None] = None,
        yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
        theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
        **options) -> Figure:
    """one bar per row

    ``x`` is the horizontal field and ``y`` the vertical one even when
    ``horizontal=True``, so a horizontal chart of sales by product is
    ``bar(rows, x="sales", y="product", horizontal=True)``.

    ``color="signed"`` colours each bar by the sign of its own value.

    See :func:`line` for the shared arguments.
    """
    frame = Frame(data, where=where)
    return _render(
        geom.Bar(label, x=x, y=y, color=color, horizontal=horizontal, width=width),
        frame, xscale, yscale, xlabel, ylabel, ax, title=title, theme=theme,
        **options,
    )


def area(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
         label: Union[str, None] = None, title: Union[str, None] = None,
         xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
         color: Union[str, None] = None, baseline: float = 0.0,
         xscale: Union[scale.Scale, None] = None,
         yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
         theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
         **options) -> Figure:
    """a filled area under a line

    args:
        baseline: value the fill drops to

    See :func:`line` for the shared arguments.
    """
    frame = Frame(data, where=where)
    return _render(
        geom.Area(label, x=x, y=y, color=color, baseline=baseline),
        frame, xscale, yscale, xlabel, ylabel, ax, title=title, theme=theme,
        **options,
    )


def stem(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
         label: Union[str, None] = None, title: Union[str, None] = None,
         xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
         color: Union[str, None] = None, baseline: bool = False,
         xscale: Union[scale.Scale, None] = None,
         yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
         theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
         **options) -> Figure:
    """a marker per row with a vertical line down to a baseline

    args:
        baseline: True draws the horizontal line at zero

    See :func:`line` for the shared arguments.
    """
    frame = Frame(data, where=where)
    return _render(
        geom.Stem(label, x=x, y=y, color=color, baseline=baseline),
        frame, xscale, yscale, xlabel, ylabel, ax, title=title, theme=theme,
        **options,
    )


def scatter(data: Rows, x: Union[str, None] = None, y: Union[str, None] = None,
            label: Union[str, None] = None, title: Union[str, None] = None,
            xlabel: Union[str, None] = None, ylabel: Union[str, None] = None,
            color: Union[str, None] = None, size: Union[float, str, None] = None,
            marker: Union[str, None] = None, xscale: Union[scale.Scale, None] = None,
            yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
            theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
            **options) -> Figure:
    """one point per row

    args:
        size: marker area in points squared, or a field name for one size per
            row

    See :func:`line` for the shared arguments.
    """
    frame = Frame(data, where=where)
    return _render(
        geom.Scatter(label, x=x, y=y, color=color, size=size, marker=marker),
        frame, xscale, yscale, xlabel, ylabel, ax, title=title, theme=theme,
        **options,
    )


def hist(data: Rows, field: str, bins: Union[int, None] = None,
         label: Union[str, None] = None, title: Union[str, None] = None,
         xlabel: Union[str, None] = None, ylabel: str = "Frequency",
         color: Union[str, None] = None, precision: int = 1,
         xscale: Union[scale.Scale, None] = None,
         yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
         theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
         **options) -> Figure:
    """a frequency distribution of one numeric field

    Binning is :class:`dkit.data.histogram.Histogram`'s job, not this module's
    and not a geom's: statistics live in :mod:`dkit.data`.  Bars are drawn at
    bin midpoints with each bin's own width, so an uneven final bin stays
    honest.

    args:
        field: numeric field to bin
        bins: number of bins.  None auto-selects by the Freedman-Diaconis rule.
        precision: retained for compatibility; bin boundaries are not rounded
        xlabel: axis label.  Defaults to ``field``, which unlike the other
            functions is worth doing here: the axis *is* the field.

    See :func:`line` for the shared arguments.
    """
    frame = Frame(data, where=where)
    histogram = Histogram.from_data(
        frame.values(field), bins=bins, precision=precision
    )
    return plot_histogram(
        histogram,
        label=label,
        title=title,
        xlabel=xlabel if xlabel is not None else field,
        ylabel=ylabel,
        color=color,
        xscale=xscale,
        yscale=yscale,
        theme=theme,
        ax=ax,
        **options,
    )


def plot_histogram(histogram, title: Union[str, None] = None,
                   label: Union[str, None] = None,
                   xlabel: Union[str, None] = None,
                   ylabel: str = "Frequency",
                   color: Union[str, None] = None,
                   theme: Union[Theme, str, None] = None,
                   ax: Union[Axes, None] = None, **options) -> Figure:
    """Render an existing Histogram as a bar chart.

    This compatibility wrapper is kept near the quick plotting API. The
    implementation lives in :mod:`dkit.plot2.histogram` to avoid duplicating
    the conversion from bins to plot rows.
    """
    from .histogram import plot_histogram as _plot_histogram

    return _plot_histogram(
        histogram,
        label=label,
        title=title,
        xlabel=xlabel,
        ylabel=ylabel,
        color=color,
        theme=theme,
        ax=ax,
        **options,
    )


def heatmap(data: Rows, x: str, y: str, z: str, label: Union[str, None] = None,
            title: Union[str, None] = None, xlabel: Union[str, None] = None,
            ylabel: Union[str, None] = None, cmap: str = "sequential",
            annotate: bool = False, format: Union[str, None] = None,
            colorbar: bool = True, xscale: Union[scale.Scale, None] = None,
            yscale: Union[scale.Scale, None] = None, where: Union[str, None] = None,
            theme: Union[Theme, str, None] = None, ax: Union[Axes, None] = None,
            **options) -> Figure:
    """a grid of two categories, shaded by a third field

    Both axes default to :class:`~dkit.plot2.scale.Categorical` rather than
    being inferred: a heat map cell covers one whole category in each direction,
    so a continuous axis is not an option to choose between.

    args:
        z: field supplying the value that colour represents
        label: names the colour bar.  A heat map has no legend to be in.
        cmap: colour map role (``"sequential"``, ``"diverging"``) or name
        annotate: True writes each cell's value inside it
        format: ``str.format`` spec for the annotations

    See :func:`line` for the shared arguments and
    :class:`~dkit.plot2.geom.matrix.HeatMap` for the rest.
    """
    return Plot(
        geom.HeatMap(label, x=x, y=y, z=z, cmap=cmap, annotate=annotate,
                     format=format, colorbar=colorbar),
        x=xscale if xscale is not None else scale.Categorical(xlabel),
        y=yscale if yscale is not None else scale.Categorical(ylabel),
        title=title, theme=theme, where=where, **options,
    ).render(data, ax=ax)


def treemap(data: Rows, label_field: str, value_field: str,
            color_field: Union[str, None] = None, title: Union[str, None] = None,
            norm: Union[str, None] = None, value_format: Union[str, None] = None,
            color_range: Union[tuple, None] = None,
            where: Union[str, None] = None, theme: Union[Theme, str, None] = None,
            ax: Union[Axes, None] = None, **options) -> Figure:
    """one rectangle per row, its area proportional to a value

    args:
        label_field: field naming each cell
        value_field: field sizing each cell
        color_field: field colouring each cell, defaulting to ``label_field``
        norm: None colours by ``color_field``; ``"linear"`` or ``"log"``
            colours by ``value_field`` and draws a colour bar
        value_format: ``str.format`` spec drawing the value under each label
        color_range: ``(vmin, vmax)`` for sequential colouring, shared between
            plots that should be comparable

    See :func:`line` for the shared arguments and
    :class:`~dkit.plot2.matplotlib_extra.TreeMap` for the styling ones, which
    ``**options`` does *not* reach: pass them to ``geom.TreeMap`` instead.
    """
    return Plot(
        geom.TreeMap(label_field, value_field, color_field, color_range=color_range,
                     norm=norm, value_format=value_format),
        title=title, theme=theme, where=where, **options,
    ).render(data, ax=ax)


def slope(data: Rows, series_field: str, pivot_field: str, value_field: str,
          title: Union[str, None] = None, ylabel: Union[str, None] = None,
          value_format: str = "{}", pivots: Union[list, None] = None,
          where: Union[str, None] = None, theme: Union[Theme, str, None] = None,
          ax: Union[Axes, None] = None, **options) -> Figure:
    """one line per series across ordered columns, labelled at both ends

    args:
        series_field: field identifying the series, one line each
        pivot_field: field supplying the columns
        value_field: field supplying the values
        ylabel: label for the value axis
        value_format: ``str.format`` spec for the values in the end labels
        pivots: column order.  None uses order of first appearance.

    See :func:`line` for the shared arguments and
    :class:`~dkit.plot2.matplotlib_extra.SlopePlot` for the styling ones.
    """
    return Plot(
        geom.Slope(series_field, pivot_field, value_field, pivots=pivots,
                   value_format=value_format),
        y=scale.Linear(ylabel), title=title, theme=theme, where=where, **options,
    ).render(data, ax=ax)


def calendar_heatmap(data: Rows, date_field: str, value_field: str,
                     start_date=None, end_date=None,
                     title: Union[str, None] = None,
                     where: Union[str, None] = None,
                     theme: Union[Theme, str, None] = None,
                     ax: Union[Axes, None] = None, **options) -> Figure:
    """one square per day, shaded by a value: a GitHub-style contribution chart

    args:
        date_field: field supplying each day's date
        value_field: field supplying the value coloured per day
        start_date: earliest date to display.  None uses the data's own
            minimum, so the span is whatever ``data`` covers.
        end_date: latest date to display.  None uses the data's own maximum.

    See :func:`line` for the shared arguments and
    :class:`~dkit.plot2.matplotlib_extra.CalendarHeatmap` for the styling
    ones, which ``**options`` does *not* reach: pass them to
    ``geom.CalendarHeatmap`` instead.
    """
    return Plot(
        geom.CalendarHeatmap(date_field, value_field, start_date=start_date,
                             end_date=end_date),
        title=title, theme=theme, where=where, **options,
    ).render(data, ax=ax)
