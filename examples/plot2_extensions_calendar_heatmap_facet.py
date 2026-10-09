"""
plot2_extensions_calendar_heatmap_facet.py
=============================================
The standalone class takes one start_date/end_date span per draw(), so
several spans side by side -- a calendar per year, here -- come from
Plot.facet() rather than from the layer itself. tight_layout is turned off:
CalendarHeatmap draws its month and day labels outside the data area, which
matplotlib's tight layout cannot always reconcile with a multi-panel grid.

day_x_margin is raised from dayplot's default (0.02): dayplot places the
day-of-week labels at that fraction of the Axes' own box width, which
shrinks to fit the panel under dayplot's fixed data aspect ratio. A facet
panel sized for the longer span (more weeks) ends up with a narrower box
than a standalone calendar of the same figure size, so the default fraction
is not enough pixels for the fixed-size label text and it is overdrawn by
the first column of day cells.

Output: plots/plot2_extensions_calendar_heatmap_facet.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import daily_activity

from dkit.plot2 import Plot, geom, save_figure


rows = [dict(r, year=r["date"].year) for r in daily_activity()]

filename = "plots/plot2_extensions_calendar_heatmap_facet.png"
fig = Plot(
    geom.CalendarHeatmap("date", "commits", day_x_margin=0.06),
    title="Daily commits by year", tight_layout=False,
).facet(rows, by="year", ncols=1, panel_width=20.0, panel_height=6.0)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
