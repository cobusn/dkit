"""
plot2_declarative_signed_bars.py
=================================
color="signed" picks the theme's positive, negative or neutral colour per
row. In dkit.plot (v1) this needed two overlapping bar series.

Output: plots/plot2_declarative_signed_bars.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


monthly = nottem()
rows = by_month(monthly)
overall_mean = sum(r["temp"] for r in monthly) / len(monthly)
for row in rows:
    row["variance"] = row["mean"] - overall_mean

filename = "plots/plot2_declarative_signed_bars.png"
Plot(
    geom.Bar("Variance", x="month", y="variance", color="signed"),
    geom.HLine(0.0),
    x=scale.Categorical("Month"),
    y=scale.Linear("Deviation from the 20 year mean (F)"),
    title="Which months run warm",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
