"""
plot2_declarative_time_axis.py
===============================
A genuine datetime x axis: scale.Time hands the axis to matplotlib's date
locator, so ticks land on real calendar boundaries rather than on row
numbers. dkit.plot (v1) had no equivalent -- it forced every axis onto
integer positions.

Output: plots/plot2_declarative_time_axis.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import Plot, geom, scale


monthly = nottem()
overall_mean = sum(r["temp"] for r in monthly) / len(monthly)

filename = "plots/plot2_declarative_time_axis.png"
Plot(
    geom.Line("Monthly mean", x="date", y="temp"),
    geom.HLine(overall_mean, label="20 year mean"),
    x=scale.Time("Year"),
    y=scale.Linear("Temperature (F)"),
    title="Nottingham Castle, 1920-1939",
).save(monthly, filename, dpi=110)
print(f"wrote {filename}")
