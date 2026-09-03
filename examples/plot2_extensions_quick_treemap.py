"""
plot2_extensions_quick_treemap.py
===================================
quick.treemap: one rectangle per row, its area proportional to the passenger
count. The cells are coloured by class, so the three blocks read as blocks
even though squarify lays them out for aspect ratio rather than by group.

Output: plots/plot2_extensions_quick_treemap.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import quick, save_figure


groups = titanic_groups(titanic())

filename = "plots/plot2_extensions_quick_treemap.png"
fig = quick.treemap(
    groups, "cell", "passengers", color_field="class",
    value_format="{:,.0f}", title="Titanic passengers by class and port",
    width=16.0, height=9.0,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
