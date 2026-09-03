"""
plot2_themes_custom.py
=======================
A custom theme derived from a bundled one: Theme.replace() states only what
differs, and keeps everything else -- fonts, figure size, grid -- from
dkit-light.

Output: plots/plot2_themes_custom.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, get_theme, scale


rows = by_month(nottem())
corporate = get_theme("dkit-light").replace(
    positive="#1b7f4b", negative="#a4243b", neutral="#8d99ae",
    number_format="{x:,.1f}",
)

filename = "plots/plot2_themes_custom.png"
Plot(
    geom.Bar("Mean", x="month", y="mean"),
    geom.Line("Warmest year", x="month", y="high", color="negative"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Custom theme",
    theme=corporate,
).save(rows, filename, dpi=110)
print(f"wrote {filename}")
