"""
plot2_extensions_treemap_log.py
=================================
norm="log" on the standalone TreeMap class. The group sizes here span 1 to
142, and on a linear ramp everything below about thirty is the same dark
colour; the log ramp separates the small groups, at the cost of overstating
their differences.

Output: plots/plot2_extensions_treemap_log.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import save_figure
from dkit.plot2.matplotlib_extra import TreeMap


groups = titanic_groups(titanic())

tm = TreeMap(norm="log", value_format="{:,.0f}", figsize=(16.0, 9.0))
fig, ax = tm.draw(groups, "cell", "passengers",
                  title="Passengers, shaded on a log ramp")

filename = "plots/plot2_extensions_treemap_log.png"
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
