"""
plot2_extensions_slope_geom.py
================================
geom.Slope inside a Plot takes its value-axis label from the Plot's y scale,
which is the one part of the ordinary scale machinery an exclusive layer
still uses.

Output: plots/plot2_extensions_slope_geom.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_decade, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_decade(nottem())

filename = "plots/plot2_extensions_slope_geom.png"
Plot(
    geom.Slope("month", "decade", "mean", value_format="{:,.1f}"),
    y=scale.Linear("Temperature (F)"),
    title="Warming by month", width=12.0, height=16.0,
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
