"""
plot2_howto_custom_geom.py
===========================
A custom geom lives in project code, not in dkit: subclass
dkit.plot2.geom.base.Geom, implement draw(ctx), and the result composes
with every built-in geom, scale and theme the same way Line or Bar do.

Dumbbell draws a low/high pair per category -- a before/after score, here --
as a line with a marker at each end. It is not a geom dkit ships, and does
not need to become one to be used like one.

See doc/source/plot2/howto_custom_geom.rst for the walk-through.

Output: plots/plot2_howto_custom_geom.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from dkit.plot2 import Plot, scale
from dkit.plot2.geom.base import Geom
from dkit.plot2.plot import RenderContext


class Dumbbell(Geom):
    """a low/high pair per category, connected by a line

    Compares a before/after pair -- or any low/high pair -- per category: a
    line between the two values with a marker at each end, read faster than
    two side-by-side bars for a "what moved" question.

    args:
        low: field supplying the lower marker's value
        high: field supplying the upper marker's value
        low_color: colour of the low marker. None uses this layer's colour.
        high_color: colour of the high marker, and of the legend entry.
            None uses this layer's colour.
        marker_size: marker size in points
        width: connecting line width in points

    See :class:`~dkit.plot2.geom.base.Geom` for the common arguments.
    """

    def __init__(self, label=None, x=None, low=None, high=None, *,
                 color=None, low_color=None, high_color=None, alpha=None,
                 marker_size=8.0, width=None, where=None, axis="left",
                 zorder=None):
        super().__init__(label, x, None, color, alpha, where, axis, zorder)
        self.low = low
        self.high = high
        self.low_color = low_color
        self.high_color = high_color
        self.marker_size = marker_size
        self.width = width

    def draw(self, ctx: RenderContext) -> None:
        x_values = ctx.x_values(self.x)
        low_values = ctx.y_values(self.low)
        high_values = ctx.y_values(self.high)
        color = self._color(ctx)
        low_color = ctx.color(self.low_color) if self.low_color else color
        high_color = ctx.color(self.high_color) if self.high_color else color

        line_kws = self._common(linewidth=self.width)
        # one legend entry for the series, not one per category's line
        line_kws.pop("label", None)
        marker_kws = self._common(s=self.marker_size ** 2)

        for xi, lo, hi in zip(x_values, low_values, high_values):
            ctx.ax.plot([xi, xi], [lo, hi], color=color, **line_kws)
        ctx.ax.scatter(
            x_values, low_values, color=low_color,
            **{k: v for k, v in marker_kws.items() if k != "label"},
        )
        ctx.ax.scatter(x_values, high_values, color=high_color, **marker_kws)


rows = [
    {"team": "Support", "before": 62, "after": 74},
    {"team": "Sales", "before": 58, "after": 55},
    {"team": "Engineering", "before": 70, "after": 81},
    {"team": "Ops", "before": 65, "after": 69},
]

filename = "plots/plot2_howto_custom_geom.png"
Plot(
    Dumbbell("After", x="team", low="before", high="after",
             low_color="neutral", high_color="highlight"),
    x=scale.Categorical("Team"),
    y=scale.Linear("Satisfaction score"),
    title="Satisfaction before and after the training programme",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
