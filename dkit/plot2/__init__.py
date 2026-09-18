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
Matplotlib plotting helpers

Toolkit for quickly building plots that share a look and feel.  Styling is
handled by :class:`dkit.plot2.theme.Theme` on top of native matplotlib
``.mplstyle`` files.

Data is always *rows*: an iterable of mappings, with fields referenced by name.

A plot is composed declaratively from layer values and drawn on demand::

    from dkit.plot2 import Plot, geom, scale

    p = Plot(
        geom.Bar("Revenue", x="month", y="revenue"),
        x=scale.Categorical("Month"),
        y=scale.Linear("Revenue"),
    )
    fig = p.render(rows)
"""
from . import scale  # noqa: F401
from .frame import Frame  # noqa: F401
from .plot import Layer, Plot, RenderContext, save_figure  # noqa: F401
from . import matplotlib_extra  # noqa: F401  (imports plot's frame and theme)
from . import geom  # noqa: F401  (imports plot and matplotlib_extra)
from . import quick  # noqa: F401  (imports geom)
from . import canned  # noqa: F401  (imports geom and dkit.data)
from .histogram import plot_histogram  # noqa: F401
from .theme import (  # noqa: F401
    CM_TO_INCH,
    Theme,
    bundled_styles,
    default_theme,
    get_theme,
    register_theme,
    resolve_style,
    set_default_theme,
    themes,
    unregister_theme,
)

__all__ = [
    "CM_TO_INCH",
    "canned",
    "Frame",
    "geom",
    "Layer",
    "matplotlib_extra",
    "Plot",
    "plot_histogram",
    "quick",
    "RenderContext",
    "Theme",
    "bundled_styles",
    "default_theme",
    "get_theme",
    "register_theme",
    "resolve_style",
    "save_figure",
    "scale",
    "set_default_theme",
    "themes",
    "unregister_theme",
]
