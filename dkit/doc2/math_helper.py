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
Render doc2 Math nodes to image files using matplotlib's mathtext.

Shared by the docx (png), reportlab (pdf) and html (svg) renderers. The
latex renderer does not use this module: it emits native $...$/$$...$$
directly.

Note: matplotlib mathtext supports a useful subset of LaTeX math, not
the full TeX math engine (no \\begin{align}, limited macros).
"""
import mimetypes
import tempfile

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from PIL import Image as PILImage

from . import document as doc

# not reliably present in the platform mimetypes database
mimetypes.add_type("image/svg+xml", ".svg")

#: matches common body-text sizes (reportlab BodyText defaults to 10pt,
#: word's Normal style to ~11pt) rather than mathtext's own larger
#: defaults, so a math span does not stand out from surrounding text
_INLINE_FONT_SIZE = 10
_DISPLAY_FONT_SIZE = 12
_DPI = 200
#: 0, not a small positive margin: any symmetric padding here stacks on
#: top of the natural space already in the surrounding text/markup
#: (e.g. "so: $...$"), visibly pushing inline math too far right
_PAD_INCHES = 0.0

#: dejavusans (mathtext's own default) renders noticeably heavier/bolder
#: than typical body-text serif fonts; cm (Computer Modern) is the family
#: LaTeX itself uses, so it also visually matches the latex renderer's
#: own native output
_MATH_FONTFAMILY = "cm"


def _fontsize(element: "doc.Math") -> int:
    return _DISPLAY_FONT_SIZE if element.display else _INLINE_FONT_SIZE


def render_math_file(element: "doc.Math", fmt: str) -> str:
    """Render a Math node to a temporary image file.

    Args:
        element: the doc2 Math node to render.
        fmt: output format, one of "svg", "pdf", "png".

    Returns:
        Path to the rendered temporary file. Caller owns cleanup.
    """
    tex = f"${element.content}$"
    fig = Figure()
    fig.text(0, 0, tex, fontsize=_fontsize(element), math_fontfamily=_MATH_FONTFAMILY)
    path = tempfile.mktemp(suffix=f".{fmt}")
    fig.savefig(
        path,
        format=fmt,
        bbox_inches="tight",
        pad_inches=_PAD_INCHES,
        dpi=_DPI,
        transparent=True,
    )
    return path


def inline_descent_pt(element: "doc.Math") -> float:
    """Distance, in points, from the rendered image's bottom edge to the
    math expression's own baseline.

    render_math_file crops tightly to each formula's own ink
    (bbox_inches="tight"), so this distance varies per formula (e.g. a
    descender like the loop in \\phi reaches further below the baseline
    than a formula with no descenders at all) -- it is not the fixed
    fraction of font size that e.g. ReportLab's own ``valign="bottom"``
    assumes. Needed to position an inline image so its *content*
    baseline, not its bounding box, lines up with the surrounding text.

    Args:
        element: the doc2 Math node (same one passed to render_math_file).

    Returns:
        Positive distance, in points, from image bottom to baseline.
    """
    tex = f"${element.content}$"
    fig = Figure(dpi=_DPI)
    FigureCanvasAgg(fig)
    text = fig.text(
        0, 0, tex, fontsize=_fontsize(element), math_fontfamily=_MATH_FONTFAMILY
    )
    fig.canvas.draw()
    bbox = text.get_window_extent(renderer=fig.canvas.get_renderer())
    # text is anchored at its own baseline (default verticalalignment),
    # at pixel y=0 in its own transform -- bbox.ymin is therefore the
    # descender depth, in pixels, below that baseline
    descent_px = -bbox.ymin
    return descent_px / _DPI * 72 + _PAD_INCHES * 72


def png_size_pt(path: str) -> tuple[float, float]:
    """Physical size, in points, of a PNG rendered by render_math_file.

    Needed by callers that embed the PNG somewhere that does not honour
    the file's own DPI metadata (e.g. ReportLab Paragraph's inline
    ``<img>`` tag, which falls back to 72dpi and renders ~2.8x too
    large at this module's _DPI otherwise).

    Args:
        path: path to a PNG previously returned by render_math_file.

    Returns:
        (width, height) in points.
    """
    with PILImage.open(path) as im:
        width_px, height_px = im.size
    return width_px / _DPI * 72, height_px / _DPI * 72
