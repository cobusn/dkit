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
Geoms: the layers a :class:`~dkit.plot2.plot.Plot` is built from

Each geom is a small class with a ``draw(ctx)`` method and no dispatch table,
which is the difference from ``dkit.plot``'s ``render_map`` keyed on a ``"~>"``
string in a serialised dict.

Every geom shares the same core arguments -- ``label``, ``x``, ``y``, ``color``,
``alpha``, ``where``, ``axis``, ``zorder`` -- documented once on
:class:`~dkit.plot2.geom.base.Geom`::

    from dkit.plot2 import Plot, geom, scale

    Plot(
        geom.Bar("Revenue", x="month", y="revenue"),
        geom.Line("Target", x="month", y="target", color="neutral", style="--"),
        x=scale.Categorical("Month"),
        y=scale.Linear("R'000"),
    )
"""
from .bar import Bar  # noqa: F401
from .base import Geom  # noqa: F401
from .line import Area, Band, Line, Stem  # noqa: F401
from .matrix import HeatMap  # noqa: F401
from .point import Scatter  # noqa: F401
from .reference import HLine, Text, VLine  # noqa: F401
from .standalone import CalendarHeatmap, Slope, TreeMap  # noqa: F401


__all__ = [
    "Area",
    "Band",
    "Bar",
    "CalendarHeatmap",
    "Geom",
    "HLine",
    "HeatMap",
    "Line",
    "Scatter",
    "Slope",
    "Stem",
    "Text",
    "TreeMap",
    "VLine",
]
