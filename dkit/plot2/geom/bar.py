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
Bar geom
"""
from typing import Union

from ..plot import RenderContext
from .base import Geom, _clean


__all__ = ["Bar"]


class Bar(Geom):
    """one bar per row

    ``x`` is always the field on the horizontal axis and ``y`` the field on the
    vertical axis, whichever way the bars point.  So a horizontal bar chart of
    sales by product is::

        Plot(
            geom.Bar("Sales", x="sales", y="product", horizontal=True),
            x=scale.Linear("Sales"),
            y=scale.Categorical("Product"),
        )

    Keeping ``x`` and ``y`` tied to the axes rather than to "category" and
    "value" is what lets the categorical scale collect its domain from the right
    field without the plot having to know each layer's orientation.

    ``color="signed"`` colours each bar by the sign of its own value, which
    replaces ``dkit.plot``'s ``GeomDelta`` and its two overlapping bar series.

    args:
        width: bar thickness, in axis units.  On a categorical axis one unit is
            one category, so the default leaves a small gap between bars.  May
            also be a field name, giving a thickness per row: that is what a
            histogram of unequal bins needs.
        horizontal: True draws bars extending along x from a baseline
        base: field name supplying the bar baseline, for stacking.  Successive
            layers take a running total field; computing that total is
            ``dkit.data``'s job, not a geom's.
        offset: shift every bar along its category axis, in axis units.  Two
            layers with ``width=0.4`` and offsets ``-0.2`` and ``0.2`` give
            grouped bars.
        edge_color: bar outline colour.  None uses the style sheet.
        edge_width: bar outline width in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, width: Union[float, str] = 0.8,
                 horizontal: bool = False, base: Union[str, None] = None,
                 offset: float = 0.0, edge_color: Union[str, None] = None,
                 edge_width: Union[float, None] = None, where: Union[str, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(label, x, y, color, alpha, where, axis, zorder)
        self.width = width
        self.horizontal = horizontal
        self.base = base
        self.offset = offset
        self.edge_color = edge_color
        self.edge_width = edge_width

    def _thickness(self, ctx: RenderContext):
        """bar thickness: a constant, or one value per row from a field"""
        if isinstance(self.width, str):
            return ctx.frame.values(self.width)
        return self.width

    def draw(self, ctx: RenderContext) -> None:
        x_values = ctx.x_values(self.x)
        y_values = ctx.y_values(self.y)
        common = self._common(
            edgecolor=self.edge_color, linewidth=self.edge_width,
        )
        thickness = self._thickness(ctx)

        if self.horizontal:
            positions = [v + self.offset for v in y_values]
            lengths = x_values
            ctx.ax.barh(
                positions, lengths, color=self._color(ctx, lengths),
                height=thickness,
                **_clean(left=ctx.x_values(self.base) if self.base else None),
                **common,
            )
        else:
            positions = [v + self.offset for v in x_values]
            lengths = y_values
            ctx.ax.bar(
                positions, lengths, color=self._color(ctx, lengths),
                width=thickness,
                **_clean(bottom=ctx.y_values(self.base) if self.base else None),
                **common,
            )
