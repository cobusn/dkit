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
Annotation layers: reference lines and anchored text

These draw at fixed positions rather than from data, so they contribute nothing
to a categorical scale's domain.  That falls out of leaving their ``x`` and ``y``
field attributes as None, which is what ``Layer.domain_values`` checks.
"""
from typing import Any, Union

from matplotlib.offsetbox import AnchoredText

from ..plot import Layer, RenderContext
from .base import Geom, _clean


__all__ = ["HLine", "Text", "VLine"]


class _ReferenceLine(Geom):
    """shared behaviour for HLine and VLine

    The default colour is the theme's ``neutral`` rather than the next cycle
    colour: a reference line drawn in series colour #3 reads as data.
    """

    def __init__(self, position: float, label: Union[str, None] = None,
                 color: Union[str, None] = None, style: Union[str, None] = "--",
                 width: Union[float, None] = None, alpha: Union[float, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(label, None, None, color, alpha, None, axis, zorder)
        self.position = position
        self.style = style
        self.width = width

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.position!r}, label={self.label!r})"

    def _line_color(self, ctx: RenderContext) -> Any:
        if self.color is None:
            return ctx.theme.neutral
        return ctx.color(self.color)


class HLine(_ReferenceLine):
    """a horizontal reference line spanning the axes

    The position comes first, not the label: unlike a geom, a reference line is
    meaningless without it.

    args:
        y: position on the y axis
        label: legend entry.  None keeps the line out of the legend.
        color: line colour.  None uses the theme's ``neutral``.
        style: line style, dashed by default so it reads as a reference
        width: line width in points
    """

    def __init__(self, y: float, label: Union[str, None] = None,
                 color: Union[str, None] = None, style: Union[str, None] = "--",
                 width: Union[float, None] = None, alpha: Union[float, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(y, label, color, style, width, alpha, axis, zorder)

    def draw(self, ctx: RenderContext) -> None:
        ctx.ax.axhline(
            self.position, color=self._line_color(ctx),
            **self._common(linestyle=self.style, linewidth=self.width),
        )


class VLine(_ReferenceLine):
    """a vertical reference line spanning the axes

    args:
        x: position on the x axis
        label: legend entry.  None keeps the line out of the legend.
        color: line colour.  None uses the theme's ``neutral``.
        style: line style, dashed by default
        width: line width in points
    """

    def __init__(self, x: float, label: Union[str, None] = None,
                 color: Union[str, None] = None, style: Union[str, None] = "--",
                 width: Union[float, None] = None, alpha: Union[float, None] = None,
                 axis: str = "left", zorder: Union[float, None] = None):
        super().__init__(x, label, color, style, width, alpha, axis, zorder)

    def draw(self, ctx: RenderContext) -> None:
        ctx.ax.axvline(
            self.position, color=self._line_color(ctx),
            **self._common(linestyle=self.style, linewidth=self.width),
        )


class Text(Layer):
    """text anchored to a corner or edge of the axes

    Replaces ``dkit.plot``'s ``AnchoredText``.  Position is a location name, not
    a data coordinate, so the text stays put whatever the data does — which is
    what the quadrant plot needs for its four corner labels.

    args:
        text: the string to draw
        location: matplotlib location name, e.g. ``"upper left"``,
            ``"lower right"``, ``"center"``
        size: font size, as points or a name like ``"small"``
        color: text colour.  None uses the style sheet's text colour.
        alpha: opacity
        frame: True draws a box around the text
        padding: padding between the text and the axes edge, in font-size units
        zorder: matplotlib draw order
    """

    def __init__(self, text: str, location: str = "upper left",
                 size: Union[float, str, None] = None, color: Union[str, None] = None,
                 alpha: Union[float, None] = None, frame: bool = False,
                 padding: float = 0.5, zorder: Union[float, None] = None):
        self.text = text
        self.location = location
        self.size = size
        self.color = color
        self.alpha = alpha
        self.frame = frame
        self.padding = padding
        self.zorder = zorder

    def __repr__(self) -> str:
        return f"Text({self.text!r}, location={self.location!r})"

    def draw(self, ctx: RenderContext) -> None:
        artist = AnchoredText(
            self.text, loc=self.location, frameon=self.frame,
            borderpad=self.padding,
            prop=_clean(size=self.size, alpha=self.alpha, color=self.color),
        )
        if self.zorder is not None:
            artist.set_zorder(self.zorder)
        ctx.ax.add_artist(artist)
