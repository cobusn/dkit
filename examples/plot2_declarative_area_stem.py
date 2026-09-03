"""
plot2_declarative_area_stem.py
===============================
Area and stem, with a vertical reference line.

Output: plots/plot2_declarative_area_stem.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_month(nottem())

filename = "plots/plot2_declarative_area_stem.png"
Plot(
    geom.Area("Warmest year", x="month", y="high"),
    geom.Stem("Coldest year", x="month", y="low", color="negative",
              baseline=True),
    geom.VLine(5.5, label="mid year"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Area and stem",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
