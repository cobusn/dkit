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
Line-family geoms: Line, Area, Band and Stem
"""
from typing import Union

from ..plot import RenderContext
from .base import Geom, _clean


__all__ = ["Area", "Band", "Line", "Stem"]


class Line(Geom):
    """a line through one point per row

    args:
        style: matplotlib line style, e.g. ``"--"``
        width: line width in points
        marker: matplotlib marker, e.g. ``"o"``.  None draws no markers.
        marker_size: marker size in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, style: Union[str, None] = None,
                 width: Union[float, None] = None, marker: Union[str, None] = None,
                 marker_size: Union[float, None] = None, where: Union[str, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(label, x, y, color, alpha, where, axis, zorder)
        self.style = style
        self.width = width
        self.marker = marker
        self.marker_size = marker_size

    def draw(self, ctx: RenderContext) -> None:
        ctx.ax.plot(
            ctx.x_values(self.x), ctx.y_values(self.y),
            color=self._color(ctx),
            **self._common(
                linestyle=self.style, linewidth=self.width,
                marker=self.marker, markersize=self.marker_size,
            ),
        )


class Area(Geom):
    """a line with the area beneath it filled

    args:
        base: field name supplying the lower edge of the fill.  None fills down
            to ``baseline``.  Give successive Area layers a running total field
            to stack them; the running total belongs in ``dkit.data``, not here.
        baseline: constant lower edge used when ``base`` is None
        fill_alpha: opacity of the fill.  The line itself stays opaque, so the
            series is still readable where two areas overlap.
        style: line style
        width: line width in points
        line: False fills without drawing the boundary line

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, base: Union[str, None] = None,
                 baseline: float = 0.0, fill_alpha: float = 0.25,
                 style: Union[str, None] = None, width: Union[float, None] = None,
                 line: bool = True, where: Union[str, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(label, x, y, color, alpha, where, axis, zorder)
        self.base = base
        self.baseline = baseline
        self.fill_alpha = fill_alpha
        self.style = style
        self.width = width
        self.line = line

    def draw(self, ctx: RenderContext) -> None:
        x_values = ctx.x_values(self.x)
        y_values = ctx.y_values(self.y)
        lower = ctx.y_values(self.base) if self.base else self.baseline
        color = self._color(ctx)

        ctx.ax.fill_between(
            x_values, lower, y_values, color=color, alpha=self.fill_alpha,
            linewidth=0, **_clean(zorder=self.zorder),
        )
        if self.line:
            # the label goes on the line, not the fill: a legend swatch at
            # fill_alpha is too faint to identify the series by
            ctx.ax.plot(
                x_values, y_values, color=color,
                **self._common(linestyle=self.style, linewidth=self.width),
            )


class Band(Geom):
    """the region between an upper and a lower series

    Replaces ``dkit.plot``'s ``GeomFill``.  Typical use is a control or
    confidence band::

        geom.Band("Limits", x="date", upper="ucl", lower="lcl",
                  color="positive", alpha=0.2)

    args:
        upper: field name of the upper edge
        lower: field name of the lower edge
        alpha: fill opacity, defaulting to 0.2 so marks drawn over the band
            stay legible
        edges: True draws the upper and lower boundary lines
        width: boundary line width in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 upper: Union[str, None] = None, lower: Union[str, None] = None,
                 color: Union[str, None] = None, alpha: float = 0.2,
                 edges: bool = False, width: Union[float, None] = None,
                 where: Union[str, None] = None, axis: str = "left",
                 zorder: Union[float, None] = None):
        super().__init__(label, x, None, color, alpha, where, axis, zorder)
        self.upper = upper
        self.lower = lower
        self.edges = edges
        self.width = width

    def draw(self, ctx: RenderContext) -> None:
        x_values = ctx.x_values(self.x)
        upper = ctx.y_values(self.upper)
        lower = ctx.y_values(self.lower)
        color = self._color(ctx)

        # the label belongs on the fill: a band *is* the patch
        ctx.ax.fill_between(
            x_values, lower, upper, color=color, linewidth=0,
            **self._common(),
        )
        if self.edges:
            for edge in (upper, lower):
                ctx.ax.plot(
                    x_values, edge, color=color,
                    **_clean(linewidth=self.width, zorder=self.zorder),
                )


class Stem(Geom):
    """a marker per row with a vertical line down to a baseline

    Replaces ``dkit.plot``'s ``GeomImpulse``, which drew bars instead.

    args:
        width: stem line width in points
        marker: matplotlib marker for the stem head
        marker_size: marker size in points
        baseline: True draws the horizontal line at zero

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, width: Union[float, None] = None,
                 marker: str = "o", marker_size: Union[float, None] = None,
                 baseline: bool = False, where: Union[str, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(label, x, y, color, alpha, where, axis, zorder)
        self.width = width
        self.marker = marker
        self.marker_size = marker_size
        self.baseline = baseline

    def draw(self, ctx: RenderContext) -> None:
        color = self._color(ctx)
        container = ctx.ax.stem(
            ctx.x_values(self.x), ctx.y_values(self.y),
            markerfmt=self.marker, **_clean(label=self.label),
        )
        # ``stem`` takes format strings rather than colour keywords, so the
        # theme's colour has to be applied to the returned artists
        for artist in (container.markerline, container.stemlines):
            artist.set_color(color)
            if self.alpha is not None:
                artist.set_alpha(self.alpha)
            if self.zorder is not None:
                artist.set_zorder(self.zorder)
        if self.width is not None:
            container.stemlines.set_linewidth(self.width)
        if self.marker_size is not None:
            container.markerline.set_markersize(self.marker_size)
        container.baseline.set_visible(self.baseline)
        if self.baseline:
            container.baseline.set_color(ctx.theme.neutral)
