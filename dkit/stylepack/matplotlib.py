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
"""Matplotlib theme construction for declarative style packs."""

from pathlib import Path

from matplotlib import font_manager

from .errors import StylePackError
from .model import StylePack


PAGE_WIDTHS_CM = {
    "a4": (21.0, 29.7),
    "letter": (21.59, 27.94),
    "legal": (21.59, 35.56),
    "a5": (14.8, 21.0),
}

_REGISTERED_FONTS: set[Path] = set()


def _register_fonts(style_pack: StylePack):
    """Register the pack's font files for the current Matplotlib process."""
    for font in style_pack.manifest.fonts:
        for resource_name in (
            font.regular,
            font.bold,
            font.italic,
            font.bold_italic,
        ):
            if resource_name is None:
                continue
            resource = style_pack.resource(resource_name)
            if resource not in _REGISTERED_FONTS:
                font_manager.fontManager.addfont(str(resource))
                _REGISTERED_FONTS.add(resource)


def matplotlib_theme(style_pack: StylePack, variant: str = "screen"):
    """Create a scoped-capable plot theme from a style pack.

    Args:
        style_pack: validated style pack.
        variant: native style variant, either ``screen`` or ``print``.

    Returns:
        An immutable :class:`dkit.plot2.theme.Theme`.

    Raises:
        StylePackError: if the pack has no requested matplotlib asset.
    """
    formats = style_pack.manifest.formats.matplotlib
    if formats is None:
        raise StylePackError(
            f"style '{style_pack.manifest.id}' has no matplotlib resources"
        )
    if variant not in ("screen", "print"):
        raise StylePackError(f"unknown matplotlib style variant '{variant}'")

    resource_name = getattr(formats, variant)
    resource = style_pack.resource(resource_name)
    _register_fonts(style_pack)
    width, _ = PAGE_WIDTHS_CM[style_pack.manifest.page.size]
    if style_pack.manifest.page.orientation == "landscape":
        width, _ = PAGE_WIDTHS_CM[style_pack.manifest.page.size][::-1]
    page = style_pack.manifest.page
    usable_width = width - page.left_margin_cm - page.right_margin_cm
    chart_width = min(style_pack.manifest.charts.width_cm, usable_width)
    typography = style_pack.manifest.typography

    from dkit.plot2.theme import Theme

    return Theme(
        rc=[
            str(Path(resource)),
            {"font.family": [typography.body]},
        ],
        categorical=style_pack.manifest.charts.categorical,
        sequential=style_pack.manifest.charts.sequential,
        diverging=style_pack.manifest.charts.diverging,
        positive=style_pack.manifest.charts.positive,
        negative=style_pack.manifest.charts.negative,
        neutral=style_pack.manifest.charts.neutral,
        highlight=style_pack.manifest.charts.highlight,
        width=chart_width,
        height=style_pack.manifest.charts.height_cm,
    )
