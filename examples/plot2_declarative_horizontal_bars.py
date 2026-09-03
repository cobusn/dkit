"""
plot2_declarative_horizontal_bars.py
=====================================
x is always the horizontal field and y the vertical one, whichever way the
bars point, so a horizontal bar chart is a Linear x and a Categorical y.

Output: plots/plot2_declarative_horizontal_bars.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


monthly = nottem()
rows = by_month(monthly)
overall_mean = sum(r["temp"] for r in monthly) / len(monthly)

filename = "plots/plot2_declarative_horizontal_bars.png"
Plot(
    geom.Bar("Mean", x="mean", y="month", horizontal=True),
    geom.VLine(overall_mean, label="20 year mean"),
    x=scale.Linear("Temperature (F)"),
    y=scale.Categorical("Month"),
    title="Horizontal bars",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
