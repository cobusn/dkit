"""
plot2_extensions_calendar_heatmap_facet.py
=============================================
The standalone class takes one start_date/end_date span per draw(), so
several spans side by side -- a calendar per year, here -- come from
Plot.facet() rather than from the layer itself. tight_layout is turned off:
CalendarHeatmap draws its month and day labels outside the data area, which
matplotlib's tight layout cannot always reconcile with a multi-panel grid.

Output: plots/plot2_extensions_calendar_heatmap_facet.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import daily_activity

from dkit.plot2 import Plot, geom, save_figure


rows = [dict(r, year=r["date"].year) for r in daily_activity()]

filename = "plots/plot2_extensions_calendar_heatmap_facet.png"
fig = Plot(
    geom.CalendarHeatmap("date", "commits"),
    title="Daily commits by year", tight_layout=False,
).facet(rows, by="year", ncols=1, panel_width=20.0, panel_height=6.0)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
