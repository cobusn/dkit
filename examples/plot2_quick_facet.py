"""
plot2_quick_facet.py
=====================
Plot.facet: one Plot, one panel per group. Scales are collected from all the
rows, so the panels are comparable -- every panel shows the same twelve
months in the same order on the same y range, which is what makes the shape
differences between decades readable.

Output: plots/plot2_quick_facet.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import Plot, geom, save_figure, scale


monthly = nottem()

seasons = Plot(
    geom.Line("Monthly mean", x="month", y="temp", marker="o", marker_size=3),
    x=scale.Categorical("Month", rotation=90),
    y=scale.Linear("Temperature (F)"),
    title="Nottingham Castle by year",
)

filename = "plots/plot2_quick_facet.png"
fig = seasons.facet(monthly, by="year", ncols=5)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
