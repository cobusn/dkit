"""
plot2_extensions_treemap_sequential.py
========================================
norm= switches the treemap from one colour per category to a colour ramp
over the value being plotted, with a colour bar to read it by. Size and
colour then say the same thing, which is only worth doing when the ranking is
the message.

Output: plots/plot2_extensions_treemap_sequential.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import quick, save_figure


groups = titanic_groups(titanic())

filename = "plots/plot2_extensions_treemap_sequential.png"
fig = quick.treemap(
    groups, "cell", "passengers", norm="linear", value_format="{:,.0f}",
    title="Passengers, shaded by count", width=16.0, height=9.0,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
