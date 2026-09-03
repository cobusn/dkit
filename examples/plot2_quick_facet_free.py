"""
plot2_quick_facet_free.py
==========================
Plot.facet with share_y=False: a smaller grid, and per-panel y ranges instead
of a shared one.

Output: plots/plot2_quick_facet_free.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import Plot, geom, save_figure, scale


monthly = nottem()

seasons = Plot(
    geom.Line("Monthly mean", x="month", y="temp", marker="o", marker_size=3),
    x=scale.Categorical("Month", rotation=90),
    y=scale.Linear("Temperature (F)"),
    title="Four years, independent y ranges",
)

filename = "plots/plot2_quick_facet_free.png"
fig = seasons.facet(
    [r for r in monthly if r["year"] < 1924],
    by="year", ncols=2, share_y=False, panel_width=8.0,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
