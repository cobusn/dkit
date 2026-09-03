"""
plot2_declarative_band_scatter.py
==================================
Layering: a band, a line, and a filtered scatter. Each layer reads the same
rows; where= filters one layer only, which is how the outlying months get
their own colour without a second data set.

Output: plots/plot2_declarative_band_scatter.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_month(nottem())

filename = "plots/plot2_declarative_band_scatter.png"
Plot(
    geom.Band("Observed range", x="month", lower="low", upper="high",
              color="neutral"),
    geom.Line("Mean", x="month", y="mean", marker="o"),
    geom.Scatter("Above 60F", x="month", y="mean", color="negative",
                 size=70, where="${mean} > 60"),
    geom.Text("shaded: monthly min to max", location="upper left"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Seasonal range",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
