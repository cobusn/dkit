"""
plot2_declarative_twin_axis.py
===============================
A twin right-hand axis: axis="right" puts a layer on a second y axis with its
own scale. Any geom can use it, and the legend still collects both axes.

Output: plots/plot2_declarative_twin_axis.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_year, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_year(nottem())

filename = "plots/plot2_declarative_twin_axis.png"
Plot(
    geom.Bar("Annual mean", x="year", y="mean"),
    geom.Line("Cumulative share", x="year", y="share", axis="right",
              color="highlight", marker="o", marker_size=3),
    x=scale.Categorical("Year", rotation=90),
    y=scale.Linear("Temperature (F)"),
    y_right=scale.Percent("Share of the period total", whole=1.0),
    title="Two scales on one plot",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
