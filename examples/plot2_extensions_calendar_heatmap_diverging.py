"""
plot2_extensions_calendar_heatmap_diverging.py
=================================================
vcenter=0 anchors zero at the midpoint of the colour map, so a day with no
net change reads as neutral regardless of how lopsided the actual gains and
losses are -- the plain sequential map in
plot2_extensions_calendar_heatmap.py would instead put "no activity" at one
end of the scale.

Output: plots/plot2_extensions_calendar_heatmap_diverging.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import daily_net_change

from dkit.plot2 import Plot, geom


rows = daily_net_change()

filename = "plots/plot2_extensions_calendar_heatmap_diverging.png"
Plot(
    geom.CalendarHeatmap("date", "net", cmap="diverging", vcenter=0, legend=True),
    title="Daily net change", width=20.0, height=6.0,
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
