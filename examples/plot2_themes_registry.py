"""
plot2_themes_registry.py
=========================
register_theme() buys reaching a theme by *name* -- from a config file, a
command line argument, or a plot written before the theme existed.
set_default_theme() buys not repeating theme= on every plot.

Output: plots/plot2_themes_registry.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import (
    Plot, geom, get_theme, register_theme, scale, set_default_theme
)


rows = by_month(nottem())
corporate = get_theme("dkit-light").replace(
    positive="#1b7f4b", negative="#a4243b", neutral="#8d99ae",
)
register_theme("corporate", corporate)
set_default_theme("corporate")

filename = "plots/plot2_themes_registry.png"
Plot(
    geom.Bar("Mean", x="month", y="mean"),
    geom.Line("Warmest year", x="month", y="high", color="negative"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Default theme, not named on the plot",
).save(rows, filename, dpi=110)
print(f"wrote {filename}")

set_default_theme(None)
