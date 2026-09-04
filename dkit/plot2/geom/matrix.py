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
Heat map geom: a third value, drawn as colour on a grid of two categories
"""
from typing import Union

import matplotlib as mpl
import numpy as np

from ...exceptions import DKitPlotException
from ..matplotlib_extra import contrast_color
from ..plot import RenderContext
from .base import Geom, _clean


__all__ = ["HeatMap"]


class HeatMap(Geom):
    """one coloured cell per ``(x, y)`` pair, shaded by a third field

    Both axes must be discrete, because a cell occupies one whole category in
    each direction::

        Plot(
            geom.HeatMap("Sales", x="month", y="region", z="sales"),
            x=scale.Categorical("Month"),
            y=scale.Categorical("Region"),
        )

    Row and column order come from the plot's domain -- order of first
    appearance across every layer -- so two runs over the same data produce the
    same picture.  ``dkit.plot``'s heat map built its axes from a ``set``, so
    the rows and columns moved between runs.

    Pairs missing from the data are left blank rather than drawn as zero: a gap
    in the grid and a real zero are different facts.  A pair appearing twice
    keeps the last row's value.

    args:
        z: field supplying the value that colour represents
        cmap: colour map role or name.  ``"sequential"`` and ``"diverging"``
            take the theme's own maps, which is what makes a set of heat maps
            match; any registered matplotlib name also works.
        vmin: value at the low end of the colour map.  None uses the data's
            own minimum.  Set both ends to compare two heat maps.
        vmax: value at the high end of the colour map
        annotate: True writes each cell's value inside it, in whichever of
            black or white is readable against that cell.
        format: ``str.format`` spec for the annotations, e.g. ``"{:,.0f}"``
        colorbar: True draws a colour bar.  ``label`` names it -- a heat map
            has no legend to be in, so that is where a layer's label goes.
        grid_color: colour of the lines between cells.  None uses the figure
            background, so cells read as separated.
        grid_width: width of those lines in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label: Union[str, None] = None, x: Union[str, None] = None,
                 y: Union[str, None] = None, z: Union[str, None] = None,
                 cmap: str = "sequential", vmin: Union[float, None] = None,
                 vmax: Union[float, None] = None, alpha: Union[float, None] = None,
                 annotate: bool = False, format: Union[str, None] = None,
                 colorbar: bool = True, grid_color: Union[str, None] = None,
                 grid_width: float = 1.0, where: Union[str, None] = None,
                 zorder: Union[float, None] = None):
        super().__init__(label, x, y, None, alpha, where, "left", zorder)
        self.z = z
        self.cmap = cmap
        self.vmin = vmin
        self.vmax = vmax
        self.annotate = annotate
        self.format = format
        self.colorbar = colorbar
        self.grid_color = grid_color
        self.grid_width = grid_width

    def _grid(self, ctx: RenderContext) -> np.ndarray:
        """the values as a masked ``(rows, columns)`` array

        Masked rather than zero-filled, so an absent pair draws nothing.
        """
        for which, scale in (("x", ctx.x), ("y", ctx.y)):
            if not scale.discrete:
                raise DKitPlotException(
                    f"HeatMap needs a discrete {which} scale: a cell covers one "
                    f"whole category. Use scale.Categorical for {which}."
                )
        columns = ctx.x_values(self.x)
        rows = ctx.y_values(self.y)
        values = ctx.frame.values(self.z)
        if not values:
            raise DKitPlotException("a heat map needs at least one row")
        # the grid spans the whole domain, not just the pairs this layer holds,
        # so a filtered layer or one facet panel still lines up with its axes
        width = len(ctx.x_domain) if ctx.x_domain else max(columns) + 1
        height = len(ctx.y_domain) if ctx.y_domain else max(rows) + 1
        grid = np.full((height, width), np.nan)
        for row, column, value in zip(rows, columns, values):
            grid[row, column] = value
        return np.ma.masked_invalid(grid)

    def draw(self, ctx: RenderContext) -> None:
        grid = self._grid(ctx)
        # cells are centred on the category positions, so their edges sit
        # halfway between: the same half unit the categorical scale pads by
        edges_x = np.arange(grid.shape[1] + 1) - 0.5
        edges_y = np.arange(grid.shape[0] + 1) - 0.5
        border = self.grid_color
        if border is None:
            border = mpl.rcParams["figure.facecolor"]

        # label is deliberately not passed to the mesh: it names the colour bar
        # instead, and a mesh handle in a legend says nothing
        mesh = ctx.ax.pcolormesh(
            edges_x, edges_y, grid,
            cmap=ctx.theme.get_cmap(self.cmap),
            edgecolors=border, linewidth=self.grid_width,
            **_clean(alpha=self.alpha, zorder=self.zorder,
                     vmin=self.vmin, vmax=self.vmax),
        )
        # the cells *are* the grid; the style sheet's grid lines would otherwise
        # show through wherever a pair is missing
        ctx.ax.grid(False)
        if self.colorbar:
            ctx.ax.figure.colorbar(mesh, ax=ctx.ax, **_clean(label=self.label))
        if self.annotate:
            self._annotate(ctx, mesh, grid)

    def _annotate(self, ctx: RenderContext, mesh, grid: np.ndarray) -> None:
        """write each cell's value inside it"""
        for row in range(grid.shape[0]):
            for column in range(grid.shape[1]):
                value = grid[row, column]
                if value is np.ma.masked:
                    continue
                text = self.format.format(value) if self.format else str(value)
                ctx.ax.text(
                    column, row, text, ha="center", va="center",
                    color=contrast_color(mesh.cmap(mesh.norm(value))),
                )
