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
Adapters putting the standalone plots inside a :class:`~dkit.plot2.plot.Plot`

:mod:`dkit.plot2.matplotlib_extra` holds two plots that own their whole Axes
and so are not layers in any real sense.  Wrapping each one as a layer anyway
buys the things :class:`~dkit.plot2.plot.Plot` provides and they do not: a theme, a title, figure
sizing, ``save()``, and -- the reason this is worth doing -- ``facet()``::

    Plot(geom.TreeMap("country", "gdp", color_field="region")) \\
        .facet(rows, by="year", ncols=3)

Because each of these owns the Axes, such a layer is *exclusive*: a Plot holding
one holds nothing else, and the plot's scales are not applied.  ``Plot`` raises
rather than silently drawing a treemap under a set of axis ticks.

Styling keywords are passed straight through to the wrapped class, which keeps
one source of truth for what they mean; ``plot=`` supplies a configured instance
instead.  Either way the instance is built once and reused for every ``draw()``,
so colours stay stable across facet panels.
"""
from typing import Sequence, Union

from .. import matplotlib_extra
from ..plot import Layer, RenderContext


__all__ = ["Slope", "TreeMap"]


class _Standalone(Layer):
    """base for the whole-axes adapters"""

    exclusive = True

    def __init__(self, cls, where: Union[str, None], plot, style: dict):
        self.where = where
        self.plot = plot if plot is not None else cls(**style)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.plot!r})"

    def domain_values(self, frame, which: str = "x") -> list:
        """these layers map nothing onto a scale, so they contribute no domain"""
        return []


class TreeMap(_Standalone):
    """a :class:`~dkit.plot2.matplotlib_extra.TreeMap` as a plot layer

    args:
        label_field: field naming each cell
        value_field: field sizing each cell
        color_field: field colouring each cell, defaulting to ``label_field``
        color_range: ``(vmin, vmax)`` for sequential colouring.  Passing the
            same range to several plots makes them comparable.
        where: filter expression applied to this layer only
        plot: a configured ``matplotlib_extra.TreeMap`` to draw with.  Share one
            between plots to keep a category's colour the same in all of them.
        **style: keyword arguments for ``matplotlib_extra.TreeMap``, e.g.
            ``norm="linear"``, ``value_format="{:,.0f}"``, ``min_label_area=0.01``
    """

    def __init__(self, label_field: str, value_field: str,
                 color_field: Union[str, None] = None, *,
                 color_range: Union[tuple, None] = None,
                 where: Union[str, None] = None, plot=None, **style):
        super().__init__(matplotlib_extra.TreeMap, where, plot, style)
        self.label_field = label_field
        self.value_field = value_field
        self.color_field = color_field
        self.color_range = color_range

    def draw(self, ctx: RenderContext) -> None:
        self.plot.draw(
            ctx.frame, self.label_field, self.value_field, self.color_field,
            color_range=self.color_range, theme=ctx.theme, ax=ctx.ax,
        )


class Slope(_Standalone):
    """a :class:`~dkit.plot2.matplotlib_extra.SlopePlot` as a plot layer

    The plot's ``y`` scale supplies the value axis label; the rest of it is not
    applied, because a slope plot draws its own ticks and column headings::

        Plot(
            geom.Slope("country", "year", "gdp"),
            y=scale.Linear("GDP, $bn"),
            title="Movement 2020 to 2024",
        )

    args:
        series_field: field identifying the series, one line each
        pivot_field: field supplying the columns
        value_field: field supplying the values
        pivots: column order.  None uses order of first appearance.
        where: filter expression applied to this layer only
        plot: a configured ``matplotlib_extra.SlopePlot`` to draw with.  Share
            one between plots to keep a series' colour the same in all of them.
        **style: keyword arguments for ``matplotlib_extra.SlopePlot``, e.g.
            ``value_format="{:,.0f}"``, ``label_width=None``
    """

    def __init__(self, series_field: str, pivot_field: str, value_field: str, *,
                 pivots: Union[Sequence, None] = None,
                 where: Union[str, None] = None, plot=None, **style):
        super().__init__(matplotlib_extra.SlopePlot, where, plot, style)
        self.series_field = series_field
        self.pivot_field = pivot_field
        self.value_field = value_field
        self.pivots = pivots

    def draw(self, ctx: RenderContext) -> None:
        self.plot.draw(
            ctx.frame, self.series_field, self.pivot_field, self.value_field,
            pivots=self.pivots, y_label=ctx.y.label, theme=ctx.theme, ax=ctx.ax,
        )
