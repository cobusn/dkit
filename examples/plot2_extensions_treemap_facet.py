"""
plot2_extensions_treemap_facet.py
===================================
One Plot, one panel per class. The layer holds a single wrapped TreeMap
instance which every panel draws through, so a port keeps its colour across
the panels -- exactly what makes the three panels comparable.

Output: plots/plot2_extensions_treemap_facet.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import Plot, geom, save_figure


groups = titanic_groups(titanic())

filename = "plots/plot2_extensions_treemap_facet.png"
fig = Plot(
    geom.TreeMap("port", "passengers", "port", value_format="{:,.0f}"),
    title="Passengers by port, per class",
).facet(groups, by="class", ncols=3, panel_width=9.0, panel_height=7.0)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
