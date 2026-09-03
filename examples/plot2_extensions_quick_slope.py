"""
plot2_extensions_quick_slope.py
=================================
quick.slope: what moved, and by how much. Every month warmed, but not
equally: the crossing lines are the point of the chart.

Output: plots/plot2_extensions_quick_slope.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_decade, nottem

from dkit.plot2 import quick, save_figure


rows = by_decade(nottem())

filename = "plots/plot2_extensions_quick_slope.png"
fig = quick.slope(
    rows, "month", "decade", "mean", value_format="{:,.1f}",
    title="Mean temperature by month, 1920s vs 1930s",
    ylabel="Temperature (F)", width=14.0, height=16.0,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
