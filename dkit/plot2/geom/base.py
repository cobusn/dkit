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
Shared behaviour for dkit.plot2 geoms

Every geom takes the same core arguments, in the same order, and resolves
colour the same way.  That consistency is most of the point of the package, so
it lives in one place rather than being restated nine times.
"""
from typing import Any, Sequence, Union

from ..plot import SIGNED, Layer, RenderContext


__all__ = ["Geom"]


def _clean(**kwargs) -> dict:
    """drop keys whose value is None

    A geom must not pass ``None`` on to matplotlib for something it has no
    opinion about: that would override the style sheet with a hardcoded default.
    Omitting the keyword lets the theme's rcParams decide.
    """
    return {k: v for k, v in kwargs.items() if v is not None}


class Geom(Layer):
    """base class for geoms

    args:
        label: legend entry.  First positional argument on every geom, so a
            legend is the default rather than an afterthought.
        x: name of the field supplying horizontal position.  None uses the
            implicit row index.
        y: name of the field supplying vertical position.
        color: a matplotlib colour, a theme semantic name (``"positive"``,
            ``"negative"``, ``"neutral"``, ``"highlight"``), ``"signed"`` for
            per-row colouring by sign where the geom supports it, or None for
            this layer's position in the theme's colour cycle.
        alpha: opacity, 0 to 1.  None uses the style sheet.
        where: filter expression applied to this layer only, e.g.
            ``"${y} > ${ucl}"``.
        axis: ``"left"`` or ``"right"``.  ``"right"`` draws against a twin y
            axis, available to every geom rather than baked into one.
        zorder: matplotlib draw order
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, where: Union[str, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        self.label = label
        self.x = x
        self.y = y
        self.color = color
        self.alpha = alpha
        self.where = where
        self.axis = axis
        self.zorder = zorder

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(label={self.label!r}, x={self.x!r}, "
            f"y={self.y!r}, color={self.color!r})"
        )

    #
    # helpers for subclasses
    #
    def _common(self, **extra) -> dict:
        """label, alpha and zorder as matplotlib keywords, Nones omitted"""
        return _clean(label=self.label, alpha=self.alpha, zorder=self.zorder, **extra)

    def _color(self, ctx: RenderContext, values: Union[Sequence, None] = None) -> Any:
        """resolve this geom's colour, as one colour or one colour per row

        args:
            ctx: the render context
            values: the values whose sign decides the colour, required only for
                ``color="signed"``
        """
        if self.color == SIGNED:
            if values is None:
                return ctx.color(SIGNED)      # raises with an explanation
            return ctx.signed_colors(values)
        return ctx.color(self.color)
