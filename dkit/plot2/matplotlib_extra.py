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
Standalone matplotlib plots for dkit.plot2

Two plots do not fit the layered model of :class:`~dkit.plot2.plot.Plot`,
because each one owns its whole Axes rather than adding marks to a pair of
scales: a treemap has no axes at all, and a slope plot draws its own column
headings, end labels and ticks.  They live here as plain objects with a
``draw()`` method.

Both follow the same shape, which is the point of putting them together:

* ``__init__`` takes the *fixed* styling -- colours, borders, label sizing,
  value formatting -- and is set once,
* ``draw()`` takes the *varying* per-plot inputs -- rows, field names, title --
  and returns ``(fig, ax)``,
* one instance can therefore be reused across any number of unrelated
  ``draw()`` calls and every plot comes out looking the same.

Colour assignment is the one thing an instance remembers.  The first time a
category is seen it is given a colour, and that colour is reused on every
later ``draw()`` -- so a series or region keeps its colour across a set of
plots, which is what makes them comparable.
:meth:`~dkit.plot2.matplotlib_extra.TreeMap.reset_colors` clears it.

Both classes accept ``ax=`` and draw into it, so they can be embedded in any
matplotlib layout, and both can be driven from a :class:`~dkit.plot2.plot.Plot` through the
adapters in :mod:`dkit.plot2.geom` -- which is how they gain themes, titles,
sizing, ``save()`` and faceting.  See :func:`dkit.plot2.quick.treemap` and
:func:`dkit.plot2.quick.slope` for the short way in.
"""
from typing import Any, Iterable, Mapping, Sequence, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
import squarify
from matplotlib.axes import Axes
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Colormap, ListedColormap, LogNorm, Normalize, to_rgb
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle

from ..exceptions import DKitPlotException
from .frame import Frame
from .theme import CM_TO_INCH, Theme, get_theme


__all__ = ["SlopePlot", "TreeMap", "contrast_color"]

#: label font size bounds for :class:`TreeMap`, as a multiple of ``font.size``.
#: A treemap label has to fit inside its cell, so it runs smaller than body
#: text; the range is what lets a large cell remain readable without shouting.
_MIN_FONT_SCALE = 0.6
_MAX_FONT_SCALE = 0.9
_TITLE_AXES_TOP = 0.90

#: inset for labels from the upper-left corner of a cell, in treemap units
_LABEL_PADDING = 1.5

#: reference axes height, in centimetres, that the bounds above are tuned for.
#: Actual sizes scale by the real axes height relative to this, so a short
#: figure gets smaller labels instead of overflowing cells.
_REFERENCE_AXES_HEIGHT_CM = 6.0

#: font sizes never shrink below this, however small the figure
_FONT_SIZE_FLOOR = 4.0

Rows = Union[Frame, Iterable[Mapping]]


def contrast_color(color) -> str:
    """black or white, whichever is readable on top of ``color``

    Used for text drawn inside a filled patch, where the fill is data driven
    and so the readable text colour cannot be fixed by the style sheet.
    """
    r, g, b = to_rgb(color)
    luminance = 0.299 * r + 0.587 * g + 0.114 * b
    return "black" if luminance > 0.5 else "white"


class _ExtraPlot:
    """shared theme and figure handling for the standalone plots

    args:
        theme: default theme, overridable per :meth:`draw` call.  None uses
            the package default.
        figsize: ``(width, height)`` in centimetres for a figure this object
            creates.  None takes the size from the theme, and then from
            ``figure.figsize``, like every other plot2 entry point.
    """

    def __init__(self, theme: Union[Theme, str, None] = None,
                 figsize: Union[tuple, None] = None):
        self.theme = theme
        self.figsize = figsize
        self._colors: dict = {}

    def reset_colors(self) -> None:
        """clear the remembered category to colour assignment"""
        self._colors = {}

    @property
    def category_colors(self) -> dict:
        """read-only view of the current category to colour assignment

        Callers building their own legend read this after drawing.
        """
        return dict(self._colors)

    def _get_theme(self, theme: Union[Theme, str, None]) -> Theme:
        """the effective theme for one draw call"""
        return get_theme(theme if theme is not None else self.theme)

    def _get_figure(self, theme: Theme, fig: Union[Figure, None],
                    ax: Union[Axes, None]) -> tuple:
        """the Axes to draw on, and its Figure

        Honours the ``ax=None`` invariant: given an Axes, nothing new is
        created, which is what makes embedding and faceting work.
        """
        if ax is not None:
            return ax.figure, ax
        if fig is not None:
            return fig, fig.add_subplot()
        if self.figsize is not None:
            width, height = self.figsize
            size = (width * CM_TO_INCH, height * CM_TO_INCH)
        else:
            # None falls through to figure.figsize, set by the style sheet
            size = theme.figsize
        return plt.subplots(figsize=size)

    def _cycle_color(self, key: Any, theme: Theme) -> Any:
        """the remembered colour for ``key``, assigning the next one if new"""
        if key not in self._colors:
            self._colors[key] = theme.color(len(self._colors))
        return self._colors[key]

    @staticmethod
    def _ordered(frame: Frame, field: str, order: Union[Sequence, None]) -> list:
        """the values of ``field`` to plot, in order

        Defaults to order of first appearance, so ordering the rows orders the
        plot and two runs over the same data agree.  ``dkit.plot`` sorted a
        ``set`` here, which reorders silently when the values are not sortable.
        """
        if order is not None:
            return list(order)
        return frame.distinct(field)


class TreeMap(_ExtraPlot):
    """squarified treemaps, one rectangle per row, sized by a value field

    ``squarify`` supplies rectangle coordinates only; everything drawn is this
    class's own, which is what allows gutters, a real colour scale and labels
    that scale with their cell::

        tm = TreeMap(value_format="${:,.0f}B")
        fig, ax = tm.draw(rows, "country", "gdp", color_field="region")

    A second ``draw()`` on different data keeps each region's colour, so the
    two plots can be read side by side.

    args:
        color_map: colour map name, a ``Colormap``, or an explicit list of
            colours.  None uses the theme: its categorical cycle when
            ``norm`` is None, its sequential map otherwise.
        norm: None colours cells by ``color_field``.  ``"linear"`` or
            ``"log"`` colours them by ``value_field`` through a colour map,
            and draws a colour bar.
        border_color: colour of the gutter drawn between cells.  None uses the
            figure's own background colour, so the cells read as separated
            rather than outlined -- and it stays right in a dark theme.
        border_width: gutter width in points
        min_label_area: fraction of the total area below which a cell's label
            is dropped rather than shrunk.  A coarse pre-filter; labels that
            still do not fit are removed after drawing, see below.
        value_format: ``str.format`` spec drawing the value on a second label
            line, e.g. ``"{:,.0f}"``.  None labels the cell with its name only.
        pad: squarify's own rectangle inset.  Distinct from ``border_width``,
            which draws on the boundary rather than shrinking the rectangle.
        font_size: fixed label size in points, or None to scale each label
            with its cell's area relative to the largest cell.
        theme: default theme, overridable per :meth:`draw`
        figsize: ``(width, height)`` in centimetres for a figure this object
            creates.  None takes the size from the theme.

    Labels are dropped, never clipped.  ``min_label_area`` cannot see that a
    long name will not fit a cell that is large but narrow, so after every
    label is placed :meth:`draw` measures what was actually rendered and
    removes any label wider or taller than its own cell.
    """

    def __init__(self, *, color_map=None, norm: Union[str, None] = None,
                 border_color: Union[str, None] = None, border_width: float = 2,
                 min_label_area: float = 0.0, value_format: Union[str, None] = None,
                 pad: bool = False, font_size: Union[float, None] = None,
                 theme: Union[Theme, str, None] = None,
                 figsize: Union[tuple, None] = None):
        super().__init__(theme, figsize)
        if norm not in (None, "linear", "log"):
            raise DKitPlotException(
                f'norm must be one of None, "linear", "log", not {norm!r}'
            )
        self.color_map = color_map
        self.norm = norm
        self.border_color = border_color
        self.border_width = border_width
        self.min_label_area = min_label_area
        self.value_format = value_format
        self.pad = pad
        self.font_size = font_size

    def __repr__(self) -> str:
        return f"TreeMap(norm={self.norm!r}, color_map={self.color_map!r})"

    #
    # colour
    #
    def _colormap(self, theme: Theme) -> Colormap:
        """the effective colour map, resolved against the active theme"""
        if self.color_map is None:
            if self.norm is None:
                return ListedColormap(list(theme.cycle()))
            return theme.get_cmap("sequential")
        if isinstance(self.color_map, Colormap):
            return self.color_map
        if isinstance(self.color_map, str):
            return theme.get_cmap(self.color_map)
        return ListedColormap(list(self.color_map))

    def _cell_colors(self, rows: Sequence[Mapping], theme: Theme, value_field: str,
                     color_field: str, color_range: Union[tuple, None]) -> tuple:
        """one colour per row, and the mappable a colour bar needs (or None)"""
        if self.norm is None:
            if self.color_map is None:
                # the theme's cycle, remembered per category: identical to what
                # every other geom does for its colour, and stable across draws
                return [self._cycle_color(r[color_field], theme) for r in rows], None
            cmap = self._colormap(theme)
            colors = []
            for row in rows:
                key = row[color_field]
                if key not in self._colors:
                    # spread assignments over the whole map, so that a
                    # continuous map used categorically still gives distinct
                    # colours instead of 256 shades of the same one
                    index = len(self._colors) % cmap.N
                    self._colors[key] = cmap(index / max(cmap.N - 1, 1))
                colors.append(self._colors[key])
            return colors, None

        values = [row[value_field] for row in rows]
        low, high = color_range if color_range is not None else (min(values), max(values))
        normalizer = (LogNorm if self.norm == "log" else Normalize)(vmin=low, vmax=high)
        cmap = self._colormap(theme)
        mappable = ScalarMappable(norm=normalizer, cmap=cmap)
        return [cmap(normalizer(v)) for v in values], mappable

    #
    # labels
    #
    def _font_size(self, area: float, max_area: float, scale: float) -> float:
        """label size for one cell: larger cell, larger label"""
        if self.font_size is not None:
            return self.font_size
        base = mpl.rcParams["font.size"]
        low = max(base * _MIN_FONT_SCALE * scale, _FONT_SIZE_FLOOR)
        high = max(base * _MAX_FONT_SCALE * scale, low)
        if max_area <= 0:
            return low
        return low + (high - low) * (area / max_area) ** 0.5

    def draw(self, data: Rows, label_field: str, value_field: str,
             color_field: Union[str, None] = None, *, title: Union[str, None] = None,
             color_range: Union[tuple, None] = None, where: Union[str, None] = None,
             theme: Union[Theme, str, None] = None, fig: Union[Figure, None] = None,
             ax: Union[Axes, None] = None) -> tuple:
        """draw one treemap and return ``(fig, ax)``

        args:
            data: rows, an iterable of mappings, or a :class:`~dkit.plot2.frame.Frame`
            label_field: field naming each cell
            value_field: field sizing each cell, and colouring it when ``norm``
                is set
            color_field: field colouring each cell, defaulting to
                ``label_field``.  Ignored when ``norm`` is set.
            title: axes title
            color_range: ``(vmin, vmax)`` for sequential colouring, instead of
                this call's own range.  Several draws sharing one range are
                comparable; a range is never remembered, because the usable
                range of a value is not known until all the data has been seen.
            where: filter expression applied to the rows
            theme: theme for this call, overriding the instance default
            fig, ax: draw into these instead of creating a figure

        returns:
            ``(fig, ax)``.  Nothing but the colour assignment is kept on self,
            so a second call cannot invalidate the figure a first one returned.
        """
        color_field = color_field or label_field
        frame = Frame(data, where=where)
        if not len(frame):
            raise DKitPlotException("a treemap needs at least one row")
        # squarify assumes descending input; the sort is stable, so equal
        # values keep the caller's row order
        rows = sorted(frame, key=lambda r: r[value_field], reverse=True)
        if min(r[value_field] for r in rows) < 0:
            raise DKitPlotException(
                f"{value_field!r} has negative values: a treemap sizes cells by "
                "area, which cannot represent them"
            )

        active = self._get_theme(theme)
        owns_axes = fig is None and ax is None
        with active.context():
            fig, ax = self._get_figure(active, fig, ax)
            self._draw_cells(fig, ax, rows, active, label_field, value_field,
                             color_field, color_range, title, owns_axes)
        return fig, ax

    def _draw_cells(self, fig: Figure, ax: Axes, rows: Sequence[Mapping], theme: Theme,
                    label_field: str, value_field: str, color_field: str,
                    color_range: Union[tuple, None], title: Union[str, None],
                    owns_axes: bool) -> None:
        """lay out and draw every cell, within an applied theme context"""
        if owns_axes:
            fig.subplots_adjust(
                left=0,
                right=1,
                bottom=0,
                top=_TITLE_AXES_TOP if title is not None else 1,
            )
        # the layout coordinate system is sized to the axes' real aspect ratio,
        # so that set_aspect("equal") below fills the axes box.  squarify's
        # square 0..100 default would otherwise be padded down to a square in
        # the middle of a wide figure, and cell areas -- which drive label
        # sizing -- would be computed against the wrong proportions
        fig.canvas.draw()
        box = ax.get_window_extent()
        width, height = 100.0, 100.0 / (box.width / box.height)

        sizes = squarify.normalize_sizes([r[value_field] for r in rows], width, height)
        layout = squarify.padded_squarify if self.pad else squarify.squarify
        rects = layout(sizes, 0, 0, width, height)
        colors, mappable = self._cell_colors(rows, theme, value_field, color_field,
                                             color_range)

        # limits and aspect before any text, so that ax.transData is accurate
        # when the fit check below asks how big a cell is on screen
        ax.set_xlim(0, width)
        ax.set_ylim(0, height)
        ax.set_aspect("equal")

        border = self.border_color
        if border is None:
            border = mpl.rcParams["figure.facecolor"]
        # label sizes are tuned for a reference height; shrink them on a
        # shorter figure, by the square root so that small plots keep labels
        # that are relatively larger and still legible
        axes_height_cm = box.height / fig.dpi / CM_TO_INCH
        font_scale = (axes_height_cm / _REFERENCE_AXES_HEIGHT_CM) ** 0.5

        total_area = width * height
        max_area = max(r["dx"] * r["dy"] for r in rects)
        labels = []

        for row, rect, color in zip(rows, rects, colors):
            ax.add_patch(Rectangle(
                (rect["x"], rect["y"]), rect["dx"], rect["dy"],
                facecolor=color, edgecolor=border, linewidth=self.border_width,
            ))
            area = rect["dx"] * rect["dy"]
            if area / total_area < self.min_label_area:
                continue
            text = str(row[label_field])
            if self.value_format is not None:
                text = f"{text}\n{self.value_format.format(row[value_field])}"
            labels.append((ax.text(
                rect["x"] + _LABEL_PADDING,
                rect["y"] + rect["dy"] - _LABEL_PADDING,
                text,
                ha="left", va="top", color=contrast_color(color),
                fontsize=self._font_size(area, max_area, font_scale),
            ), rect))

        if mappable is not None:
            fig.colorbar(mappable, ax=ax)
        if title is not None:
            ax.set_title(title)
        ax.axis("off")
        self._drop_overflowing_labels(fig, ax, labels)

    @staticmethod
    def _drop_overflowing_labels(fig: Figure, ax: Axes, labels: Sequence) -> None:
        """remove every label whose rendered box is bigger than its cell"""
        if not labels:
            return
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        for artist, rect in labels:
            extent = artist.get_window_extent(renderer=renderer)
            (x0, y0), (x1, y1) = ax.transData.transform([
                (rect["x"], rect["y"]),
                (rect["x"] + rect["dx"], rect["y"] + rect["dy"]),
            ])
            if extent.width > abs(x1 - x0) or extent.height > abs(y1 - y0):
                artist.remove()


class SlopePlot(_ExtraPlot):
    """one line per series across ordered columns, labelled at both ends

    The plot for "who moved, and by how much" between two or three points in
    time.  It carries no legend and almost no axis furniture, because each
    line is labelled where it starts and where it ends::

        sp = SlopePlot(value_format="{:,.0f}")
        fig, ax = sp.draw(rows, series_field="country", pivot_field="year",
                          value_field="gdp")

    A series keeps its colour across ``draw()`` calls, so a set of these can
    be read together.

    args:
        value_format: ``str.format`` spec for the values in the end labels
        label_width: truncate series names in the end labels to this many
            characters.  None leaves them whole.
        marker_size: size of the point drawn at each column
        label_size: font size for the end labels and column headings.  None
            derives one from ``font.size``.
        margin: horizontal room left for the end labels, in column widths.
            The labels sit outside the data, so this is what keeps them on the
            figure.
        y_pad: fraction of the value range left as vertical margin
        theme: default theme, overridable per :meth:`draw`
        figsize: ``(width, height)`` in centimetres for a figure this object
            creates.  None takes the size from the theme.

    A series missing a column is drawn with a gap there rather than dropping
    to zero, and its end labels attach to its own first and last *present*
    values.  ``dkit.plot``'s version labelled the first and last column
    unconditionally, so a series absent from either end went unlabelled, and
    it tested values for truth, which silently dropped a label worth 0.
    """

    def __init__(self, *, value_format: str = "{}", label_width: Union[int, None] = 16,
                 marker_size: float = 12, label_size: Union[float, None] = None,
                 margin: float = 1.0, y_pad: float = 0.1,
                 theme: Union[Theme, str, None] = None,
                 figsize: Union[tuple, None] = None):
        super().__init__(theme, figsize)
        self.value_format = value_format
        self.label_width = label_width
        self.marker_size = marker_size
        self.label_size = label_size
        self.margin = margin
        self.y_pad = y_pad

    def __repr__(self) -> str:
        return f"SlopePlot(value_format={self.value_format!r})"

    def _label(self, name: Any, value: float) -> str:
        text = str(name)
        if self.label_width is not None:
            text = text[:self.label_width]
        return f"{text}, {self.value_format.format(value)}"

    def draw(self, data: Rows, series_field: str, pivot_field: str, value_field: str,
             *, title: Union[str, None] = None, y_label: Union[str, None] = None,
             pivots: Union[Sequence, None] = None, where: Union[str, None] = None,
             theme: Union[Theme, str, None] = None, fig: Union[Figure, None] = None,
             ax: Union[Axes, None] = None) -> tuple:
        """draw one slope plot and return ``(fig, ax)``

        args:
            data: rows, an iterable of mappings, or a :class:`~dkit.plot2.frame.Frame`
            series_field: field identifying the series, one line each
            pivot_field: field supplying the columns
            value_field: field supplying the values
            title: axes title
            y_label: label for the value axis
            pivots: column order.  None uses order of first appearance, so
                ordering the rows orders the columns.
            where: filter expression applied to the rows
            theme: theme for this call, overriding the instance default
            fig, ax: draw into these instead of creating a figure

        returns:
            ``(fig, ax)``
        """
        frame = Frame(data, where=where)
        if not len(frame):
            raise DKitPlotException("a slope plot needs at least one row")
        columns = self._ordered(frame, pivot_field, pivots)
        series = {
            key: {r[pivot_field]: r[value_field] for r in group}
            for key, group in frame.groups(series_field).items()
        }
        values = [
            v for points in series.values()
            for pivot, v in points.items() if pivot in columns and v is not None
        ]
        if not values:
            raise DKitPlotException(
                f"no values to plot: no row has both {pivot_field!r} in the "
                f"column order and a {value_field!r}"
            )

        active = self._get_theme(theme)
        with active.context():
            fig, ax = self._get_figure(active, fig, ax)
            self._draw_series(ax, active, columns, series, values, title, y_label)
        return fig, ax

    def _draw_series(self, ax: Axes, theme: Theme, columns: list, series: dict,
                     values: list, title: Union[str, None],
                     y_label: Union[str, None]) -> None:
        """draw the columns, lines and labels, within an applied theme context"""
        low, high = min(values), max(values)
        span = (high - low) or abs(high) or 1.0
        pad = span * self.y_pad
        ax.set_xlim(-self.margin, len(columns) - 1 + self.margin)
        ax.set_ylim(low - pad, high + pad)

        size = self.label_size
        if size is None:
            size = max(mpl.rcParams["font.size"] - 1, _FONT_SIZE_FLOOR)

        # the columns themselves: a dotted rule each, and a heading below the
        # data.  The rules are what turn a set of lines into a comparison
        ax.vlines(
            range(len(columns)), low, high, linestyles="dotted", linewidth=1,
            color=mpl.rcParams["axes.edgecolor"], alpha=0.7,
        )
        for index, name in enumerate(columns):
            ax.text(index, low - pad * 0.25, str(name), ha="center", va="top",
                    fontsize=size, fontweight="bold")

        for name, points in series.items():
            color = self._cycle_color(name, theme)
            y = [points.get(c) for c in columns]
            ax.plot(range(len(columns)), y, color=color, marker="o",
                    markersize=self.marker_size ** 0.5, label=str(name))
            # label each end where the series actually has a value, so a
            # series that starts or ends late is still named
            present = [i for i, v in enumerate(y) if v is not None]
            if not present:
                continue
            first, last = present[0], present[-1]
            ax.text(first - 0.05, y[first], self._label(name, y[first]),
                    ha="right", va="center", fontsize=size, color=color)
            if last != first:
                ax.text(last + 0.05, y[last], self._label(name, y[last]),
                        ha="left", va="center", fontsize=size, color=color)

        # the headings replace the x ticks, and only the extremes of the value
        # range carry information here
        ax.set_xticks([])
        ax.set_yticks([low, high])
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.grid(False)
        if y_label is not None:
            ax.set_ylabel(y_label)
        if title is not None:
            ax.set_title(title)
