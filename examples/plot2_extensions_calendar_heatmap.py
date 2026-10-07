"""
plot2_extensions_calendar_heatmap.py
======================================
geom.CalendarHeatmap: one square per day, shaded by value, GitHub-contribution
style. The span is whatever the data covers -- 440 days here, not a calendar
year -- because start_date/end_date default to the data's own min and max.

Output: plots/plot2_extensions_calendar_heatmap.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import daily_activity

from dkit.plot2 import Plot, geom


rows = daily_activity()

filename = "plots/plot2_extensions_calendar_heatmap.png"
Plot(
    geom.CalendarHeatmap("date", "commits", legend=True),
    title="Daily commits", width=20.0, height=6.0,
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
