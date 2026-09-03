"""
plot2_extensions_treemap_geom.py
==================================
geom.TreeMap inside a Plot buys what the standalone class does not have: a
theme resolved by name, a title, a figure size in centimetres, where=, save()
and facet(). Everything else is passed straight through to the wrapped
class.

Output: plots/plot2_extensions_treemap_geom.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import Plot, geom


groups = titanic_groups(titanic())

filename = "plots/plot2_extensions_treemap_geom.png"
Plot(
    geom.TreeMap("cell", "passengers", "class", value_format="{:,.0f}"),
    title="Titanic passengers (dkit-dark)",
    theme="dkit-dark", width=16.0, height=9.0,
).save(groups, filename, dpi=110)
print(f"wrote {filename}")
