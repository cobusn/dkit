"""
plot2_declarative_grouped_bars.py
==================================
Grouped bars: two layers at half width, shifted by offset=. Neither grouping
nor stacking is a mode on the geom -- anything you can compute you can lay
out this way.

Output: plots/plot2_declarative_grouped_bars.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_month(nottem())

filename = "plots/plot2_declarative_grouped_bars.png"
Plot(
    geom.Bar("Coldest year", x="month", y="low", width=0.4, offset=-0.2),
    geom.Bar("Warmest year", x="month", y="high", width=0.4, offset=0.2),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Grouped bars",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
