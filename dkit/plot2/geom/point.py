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
Point geom
"""
from typing import Union

from ..plot import RenderContext
from .base import Geom


__all__ = ["Scatter"]


class Scatter(Geom):
    """one marker per row

    ``where=`` is what makes this useful as an overlay: a scatter of only the
    out-of-control points on top of a line of all of them is::

        geom.Scatter("Out of control", x="date", y="y", color="negative",
                     where="${y} > ${ucl} | ${y} < ${lcl}")

    args:
        size: marker area in points squared, or a field name to size each marker
            by its own value
        marker: matplotlib marker
        edge_color: marker outline colour
        edge_width: marker outline width in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None,
                 size: Union[float, str, None] = None, marker: Union[str, None] = None,
                 edge_color: Union[str, None] = None,
                 edge_width: Union[float, None] = None,
                 where: Union[str, None] = None, axis: str = "left",
                 zorder: Union[float, None] = None):
        super().__init__(label, x, y, color, alpha, where, axis, zorder)
        self.size = size
        self.marker = marker
        self.edge_color = edge_color
        self.edge_width = edge_width

    def draw(self, ctx: RenderContext) -> None:
        y_values = ctx.y_values(self.y)
        sizes = ctx.frame.values(self.size) if isinstance(self.size, str) else self.size
        ctx.ax.scatter(
            ctx.x_values(self.x), y_values,
            c=self._color(ctx, y_values),
            **self._common(
                s=sizes, marker=self.marker,
                edgecolors=self.edge_color, linewidths=self.edge_width,
            ),
        )
