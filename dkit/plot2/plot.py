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
Declarative plot composition for dkit.plot2

A :class:`~dkit.plot2.plot.Plot` is a *reusable specification*.  Layers are values, nothing is
drawn until :meth:`~dkit.plot2.plot.Plot.render` is called, and rendering does not consume or
modify the spec::

    p = Plot(
        geom.Line("Actual", x="date", y="value"),
        x=scale.Time("Date"),
        y=scale.Linear("Value"),
        title="Monthly Volume",
    )
    fig = p.render(rows)             # reusable: p.render(other_rows)
    p2 = p.add(geom.Line(...))       # a NEW Plot; p is unchanged

This replaces ``dkit.plot``'s ``+`` operator, which was implemented as
``other.modify(self); return self`` and so mutated its left operand: a "base"
plot silently acquired every layer later added to any plot derived from it.

Two invariants hold for every entry point here:

* **``ax=None`` is always accepted.**  Given an Axes, a Plot draws into it and
  creates no figure of its own, which is what makes faceting and embedding into
  an existing figure possible.
* **The theme is applied scoped.**  Rendering happens inside
  ``theme.context()``, so a themed plot leaves global rcParams untouched and
  cannot restyle later plots.
"""
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Iterable, Mapping, Sequence, Union

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from ..exceptions import DKitPlotException
from .frame import Frame
from .scale import Linear, Scale
from .theme import CM_TO_INCH, Theme, get_theme


__all__ = ["Layer", "Plot", "RenderContext", "save_figure"]

#: semantic colour names understood by ``color=`` on any layer
SEMANTIC_COLORS = ("positive", "negative", "neutral", "highlight")

#: ``color="signed"`` colours each mark by the sign of its own value.  Only
#: geoms that draw one mark per row can honour it.
SIGNED = "signed"

#: valid values for ``axis=`` on a layer
AXES = ("left", "right")


@dataclass
class RenderContext:
    """everything a layer needs in order to draw itself

    One context is built per layer per render, so a layer holds no state of its
    own and the same layer object can appear in several plots.

    attributes:
        ax: the Axes this layer draws on: the primary axes, or the twin created
            for ``axis="right"``
        frame: rows, already filtered by both the plot's and the layer's
            ``where``
        theme: the active theme
        x: the x scale
        y: the y scale for this layer's axis
        x_domain: ordered distinct x values, when the x scale is discrete
        y_domain: ordered distinct y values, when the y scale is discrete
        index: position of this layer in the plot, used for the default colour
    """
    ax: Axes
    frame: Frame
    theme: Theme
    x: Scale
    y: Scale
    x_domain: Union[list, None] = None
    y_domain: Union[list, None] = None
    index: int = 0

    def color(self, spec: Union[str, None] = None) -> str:
        """resolve a layer colour specification

        args:
            spec: a semantic name (``"positive"``, ``"negative"``,
                ``"neutral"``, ``"highlight"``), any matplotlib colour, or None
                for this layer's position in the theme's categorical cycle

        raises:
            DKitPlotException: for ``"signed"``, which needs one colour per
                row and so cannot be resolved to a single colour here
        """
        if spec is None:
            return self.theme.color(self.index)
        if spec in SEMANTIC_COLORS:
            return getattr(self.theme, spec)
        if spec == SIGNED:
            raise DKitPlotException(
                "color='signed' colours one mark per row and is only supported "
                "by geoms that draw per-row marks, such as Bar and Scatter"
            )
        return spec

    def signed_colors(self, values: Sequence[float]) -> list:
        """one semantic colour per value, by sign

        Replaces ``dkit.plot``'s ``GeomDelta``, which drew two overlapping bar
        series to get the same effect.
        """
        return [self.theme.signed(v) for v in values]

    def x_values(self, field: Union[str, None] = None) -> list:
        """x coordinates for ``field``, encoded through the x scale

        An omitted field falls back to the implicit row index.
        """
        return self.x.encode(self.frame.values(field), self.x_domain)

    def y_values(self, field: Union[str, None] = None) -> list:
        """y coordinates for ``field``, encoded through this layer's y scale"""
        return self.y.encode(self.frame.values(field), self.y_domain)


class Layer(ABC):
    """base class for everything that draws onto a plot

    Subclasses live in :mod:`dkit.plot2.geom`.  A layer holds only its own
    configuration; all per-render state arrives in the :class:`RenderContext`,
    which is what makes a layer safe to share between plots.

    attributes:
        label: legend entry.  Every layer's first positional argument, because
            ``dkit.plot`` never passed ``label=`` to matplotlib and so could not
            produce a legend at all.
        where: optional filter expression applied to this layer only
        axis: ``"left"`` or ``"right"``.  ``"right"`` draws on a twin y axis,
            available to any layer rather than hardcoded into one geom.
        x: name of the x field, or None for the implicit row index
        y: name of the y field
        exclusive: True for a layer that owns its whole Axes -- a treemap, a
            slope plot -- rather than adding marks to a pair of scales.  Such a
            layer cannot share a plot, and the plot's scales are not applied to
            it.  See :mod:`dkit.plot2.geom.standalone`.
    """
    label: Union[str, None] = None
    where: Union[str, None] = None
    axis: str = "left"
    x: Union[str, None] = None
    y: Union[str, None] = None
    exclusive: ClassVar[bool] = False

    @abstractmethod
    def draw(self, ctx: RenderContext) -> None:
        """draw this layer onto ``ctx.ax``"""

    def domain_values(self, frame: Frame, which: str = "x") -> list:
        """raw values this layer contributes to a discrete scale

        Layers that do not map data onto an axis, such as a horizontal
        reference line, override this to return an empty list.
        """
        field = self.x if which == "x" else self.y
        if field is None:
            return []
        return frame.values(field)


@dataclass(frozen=True, init=False)
class Plot:
    """a reusable plot specification

    args:
        *layers: layer objects, drawn in the order given
        x: x scale, defaults to :class:`~dkit.plot2.scale.Linear`
        y: y scale for the left axis
        y_right: y scale for the twin right axis.  Created automatically when
            any layer sets ``axis="right"``.
        title: axes title
        theme: a :class:`~dkit.plot2.theme.Theme`, the name of one, or None for
            the default
        legend: True always draws a legend, False never does, None draws one
            when more than one layer is labelled
        legend_loc: matplotlib legend location
        where: filter expression applied to the data before any layer sees it
        width: figure width in centimetres, overriding the theme
        height: figure height in centimetres, overriding the theme
        tight_layout: call ``Figure.tight_layout()`` after drawing, to avoid
            clipping rotated tick labels and the like.  Only applies when
            this plot draws into a figure it created itself -- when ``ax=``
            is supplied to :meth:`render`/:meth:`save`, layout is the
            caller's responsibility, since the figure may hold other axes.
    """
    layers: tuple
    x: Scale
    y: Scale
    y_right: Union[Scale, None]
    title: Union[str, None]
    theme: Union[Theme, str, None]
    legend: Union[bool, None]
    legend_loc: str
    where: Union[str, None]
    width: Union[float, None]
    height: Union[float, None]
    tight_layout: bool

    def __init__(self, *layers: Layer, x: Union[Scale, None] = None,
                 y: Union[Scale, None] = None, y_right: Union[Scale, None] = None,
                 title: Union[str, None] = None,
                 theme: Union[Theme, str, None] = None,
                 legend: Union[bool, None] = None, legend_loc: str = "best",
                 where: Union[str, None] = None, width: Union[float, None] = None,
                 height: Union[float, None] = None, tight_layout: bool = True):
        flat = tuple(_flatten(layers))
        for layer in flat:
            if not isinstance(layer, Layer):
                raise DKitPlotException(
                    f"{type(layer).__name__} is not a plot layer"
                )
            if layer.axis not in AXES:
                raise DKitPlotException(
                    f"layer axis must be one of {AXES}, not {layer.axis!r}"
                )
            if layer.exclusive and len(flat) > 1:
                raise DKitPlotException(
                    f"{type(layer).__name__} owns the whole axes and cannot share "
                    f"a plot with {len(flat) - 1} other layer(s)"
                )
        set_ = object.__setattr__
        set_(self, "layers", flat)
        set_(self, "x", x if x is not None else Linear())
        set_(self, "y", y if y is not None else Linear())
        set_(self, "y_right", y_right)
        set_(self, "title", title)
        set_(self, "theme", theme)
        set_(self, "legend", legend)
        set_(self, "legend_loc", legend_loc)
        set_(self, "where", where)
        set_(self, "width", width)
        set_(self, "height", height)
        set_(self, "tight_layout", tight_layout)

    #
    # composition: always returns a new Plot
    #
    def add(self, *layers: Layer) -> "Plot":
        """a new Plot with ``layers`` appended.  This plot is unchanged."""
        return self.replace(layers=self.layers + tuple(_flatten(layers)))

    def replace(self, **kwargs) -> "Plot":
        """a new Plot with the given options changed"""
        layers = kwargs.pop("layers", self.layers)
        options = {
            "x": self.x, "y": self.y, "y_right": self.y_right, "title": self.title,
            "theme": self.theme, "legend": self.legend, "legend_loc": self.legend_loc,
            "where": self.where, "width": self.width, "height": self.height,
            "tight_layout": self.tight_layout,
        }
        unknown = set(kwargs) - set(options)
        if unknown:
            raise DKitPlotException(f"unknown plot option: {', '.join(sorted(unknown))}")
        options.update(kwargs)
        return Plot(*layers, **options)

    #
    # rendering
    #
    @property
    def exclusive(self) -> bool:
        """True if this plot's one layer owns the whole axes

        Such a layer draws its own axis furniture, so the scales are left alone
        and no legend is drawn.  See :mod:`dkit.plot2.geom.standalone`.
        """
        return bool(self.layers) and self.layers[0].exclusive

    def get_theme(self) -> Theme:
        """the effective theme, with ``width`` and ``height`` applied"""
        theme = get_theme(self.theme)
        if self.width is not None or self.height is not None:
            theme = theme.replace(
                width=self.width if self.width is not None else theme.width,
                height=self.height if self.height is not None else theme.height,
            )
        return theme

    def render(self, data: Union[Frame, Iterable[Mapping]],
               ax: Union[Axes, None] = None) -> Figure:
        """draw this plot and return the Figure

        args:
            data: rows, an iterable of mappings, or a :class:`~dkit.plot2.frame.Frame`
            ax: draw into this Axes instead of creating a figure

        returns:
            the Figure drawn on, either a new one or ``ax.figure``
        """
        theme = self.get_theme()
        with theme.context():
            return self._draw(data, ax, theme)

    def save(self, data: Union[Frame, Iterable[Mapping]], filename: str, **kwargs) -> None:
        """render to ``filename`` and close the figure

        Saving happens inside the theme's context, so ``savefig.*`` rcParams
        from the style sheet apply.

        args:
            data: rows, an iterable of mappings, or a Frame
            filename: output file, format inferred from the extension
            **kwargs: passed to ``Figure.savefig``
        """
        theme = self.get_theme()
        with theme.context():
            fig = self._draw(data, None, theme)
            fig.savefig(filename, **kwargs)
            plt.close(fig)

    def facet(self, data: Union[Frame, Iterable[Mapping]], by: str, ncols: int = 2,
              share_x: bool = True, share_y: bool = True, titles: bool = True,
              panel_width: Union[float, None] = None,
              panel_height: Union[float, None] = None) -> Figure:
        """draw one panel per distinct value of ``by`` on a shared grid

        The plot is defined once and drawn once per group, which is what the
        ``ax=`` escape hatch on :meth:`render` exists for::

            Plot(geom.Line(x="month", y="temp"), x=scale.Categorical("Month")) \\
                .facet(rows, by="year", ncols=4)

        Scales are shared, not recomputed per panel: a discrete domain is
        collected from *all* the rows, so category 3 is in the same place in
        every panel even in a group where it does not occur.  This is the
        difference between comparable panels and twelve unrelated plots.

        args:
            data: rows, an iterable of mappings, or a :class:`~dkit.plot2.frame.Frame`
            by: field to group on.  Panels follow first-seen order, so ordering
                the rows orders the panels.
            ncols: panels per row
            share_x: share the x axis across panels, drawing tick labels only on
                the bottom row
            share_y: share the y axis across panels, drawing tick labels only in
                the first column
            titles: True titles each panel with its group value.  This plot's own
                ``title`` becomes the figure title.
            panel_width: width of one panel in centimetres.  None derives a
                readable default from the theme.
            panel_height: height of one panel in centimetres

        returns:
            the Figure
        """
        if ncols < 1:
            raise DKitPlotException(f"ncols must be at least 1, not {ncols}")
        theme = self.get_theme()
        with theme.context():
            frame = Frame(data, where=self.where)
            groups = frame.groups(by)
            if not groups:
                raise DKitPlotException(f"no rows to facet by {by!r}")
            nrows = math.ceil(len(groups) / ncols)
            fig, axes = plt.subplots(
                nrows, ncols, squeeze=False, sharex=share_x, sharey=share_y,
                figsize=_facet_figsize(theme, ncols, nrows, panel_width, panel_height),
            )
            flat = [a for row in axes for a in row]

            # domains come from every row, not from each group, so that panels
            # stay comparable
            domains = {
                "x": self._domain(frame, "x"),
                "y": self._domain(frame, "y"),
            }
            for index, (key, group) in enumerate(groups.items()):
                ax = flat[index]
                # the last ncols panels are the bottom-most in their column,
                # including the column that a partly filled last row leaves short
                bottom = index >= len(groups) - ncols
                self._draw_axes(
                    group, ax, theme, domains,
                    title=str(key) if titles else None,
                    # one legend for the whole figure reads better than n copies
                    legend=index == 0,
                    # an inner panel repeating the axis label is noise
                    label_x=not share_x or bottom,
                    label_y=not share_y or index % ncols == 0,
                )
                if share_x and bottom:
                    # sharex hides tick labels on every row but the last, which
                    # strands a bottom-most panel sitting above a removed one
                    ax.tick_params(labelbottom=True)
            for ax in flat[len(groups):]:
                ax.remove()

            if self.title is not None:
                fig.suptitle(self.title)
            if self.tight_layout:
                fig.tight_layout()
            return fig

    def _draw(self, data, ax: Union[Axes, None], theme: Theme) -> Figure:
        """draw within an already applied theme context"""
        frame = Frame(data, where=self.where)

        owns_figure = ax is None
        if owns_figure:
            fig, ax = plt.subplots(figsize=theme.figsize)
        else:
            fig = ax.figure
            theme.size_figure(fig)

        self._draw_axes(
            frame, ax, theme,
            {"x": self._domain(frame, "x"), "y": self._domain(frame, "y")},
            title=self.title,
        )
        if owns_figure and self.tight_layout:
            fig.tight_layout()
        return fig

    def _draw_axes(self, frame: Frame, ax: Axes, theme: Theme, domains: dict,
                   title: Union[str, None] = None, legend: bool = True,
                   label_x: bool = True, label_y: bool = True) -> None:
        """draw every layer and configure both scales on one Axes

        Shared by :meth:`_draw` and :meth:`facet`; the facet case supplies
        domains collected from all groups and suppresses repeated labels.
        """
        right = self._right_axes(ax)

        for index, layer in enumerate(self.layers):
            target = right if layer.axis == "right" else ax
            y_scale = self._y_scale(layer.axis)
            layer.draw(RenderContext(
                ax=target,
                frame=frame.filter(layer.where),
                theme=theme,
                x=self.x,
                y=y_scale,
                x_domain=domains["x"],
                y_domain=domains["y"] if layer.axis == "left" else None,
                index=index,
            ))

        if not self.exclusive:
            x_scale = self.x if label_x else self.x.replace(label=None)
            y_scale = self.y if label_y else self.y.replace(label=None)
            x_scale.configure(ax, "x", theme, domains["x"])
            y_scale.configure(ax, "y", theme, domains["y"])
            if right is not None:
                self._y_scale("right").configure(right, "y", theme)

        if title is not None:
            ax.set_title(title)

        if legend and not self.exclusive:
            self._draw_legend(ax, right)

    def _right_axes(self, ax: Axes) -> Union[Axes, None]:
        """twin y axes, or None if nothing needs one"""
        if self.y_right is None and not any(la.axis == "right" for la in self.layers):
            return None
        right = ax.twinx()
        # the style sheet hides the right spine, which the twin does need,
        # and a second grid over the first is noise
        right.spines["right"].set_visible(True)
        right.grid(False)
        return right

    def _y_scale(self, axis: str) -> Scale:
        if axis == "right":
            return self.y_right if self.y_right is not None else Linear()
        return self.y

    def _domain(self, frame: Frame, which: str) -> Union[list, None]:
        """ordered distinct values for a discrete scale, else None"""
        scale = self.x if which == "x" else self.y
        if not scale.discrete:
            return None
        values: list = []
        for layer in self.layers:
            if which == "y" and layer.axis != "left":
                continue
            values.extend(layer.domain_values(frame.filter(layer.where), which))
        # nothing contributed: the domain is unknown, not empty.  An empty list
        # would make every encode() raise, so leave each layer to derive its own.
        return list(dict.fromkeys(values)) or None

    def _draw_legend(self, ax: Axes, right: Union[Axes, None]) -> None:
        handles: list = []
        labels: list = []
        for target in (ax, right):
            if target is None:
                continue
            h, la = target.get_legend_handles_labels()
            handles.extend(h)
            labels.extend(la)
        if self.legend is False:
            return
        minimum = 1 if self.legend else 2
        if len(labels) >= minimum:
            ax.legend(handles, labels, loc=self.legend_loc)


#: default panel size for a facet grid, as a fraction of a single plot's size.
#: A whole figure per panel makes a four-column grid unusable on screen.
PANEL_SCALE = 0.6


def _facet_figsize(theme: Theme, ncols: int, nrows: int,
                   panel_width: Union[float, None],
                   panel_height: Union[float, None]) -> tuple:
    """figure size in inches for a grid of panels

    Panels default to a fraction of one plot's size.  ``Plot(width=, height=)``
    is not used here: it means "the whole figure", and honouring it would give
    panels that shrink as groups are added.
    """
    default = mpl.rcParams["figure.figsize"]
    width = panel_width * CM_TO_INCH if panel_width else default[0] * PANEL_SCALE
    height = panel_height * CM_TO_INCH if panel_height else default[1] * PANEL_SCALE
    return (width * ncols, height * nrows)


def save_figure(fig: Figure, filename: str, theme: Union[Theme, str, None] = None,
                close: bool = True, **kwargs) -> None:
    """save a figure inside a theme's context, then close it

    :meth:`~dkit.plot2.plot.Plot.save` covers the common case, but the ``quick`` functions and
    the tier C classes *return* a figure, and a caller who saves it later is
    outside the theme's context by then.  ``savefig.*`` rcParams -- including
    ``savefig.bbox: tight``, which the bundled styles set -- come from the
    style sheet, so a plain ``fig.savefig(...)`` at that point silently loses
    them and clips axis labels.  This applies them again for the save::

        fig = quick.bar(rows, x="month", y="sales")
        save_figure(fig, "sales.png")

    args:
        fig: the figure to save
        filename: output file, format inferred from the extension
        theme: theme whose ``savefig.*`` params to apply, or None for the
            default.  Pass the same theme the figure was drawn with when it is
            not the default one, since that is where its background colour
            comes from.
        close: True closes the figure afterwards, which is what a script
            writing many files wants.  False keeps it, for saving twice.
        **kwargs: passed to ``Figure.savefig``, and win over the rcParams
    """
    with get_theme(theme).context():
        fig.savefig(filename, **kwargs)
    if close:
        plt.close(fig)


def _flatten(layers: Sequence[Any]) -> Iterable[Any]:
    """allow both ``Plot(a, b)`` and ``Plot(*layers)`` or ``Plot([a, b])``"""
    for item in layers:
        if isinstance(item, Layer) or not isinstance(item, (list, tuple)):
            yield item
        else:
            yield from _flatten(item)
