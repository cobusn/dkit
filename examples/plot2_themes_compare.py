"""
plot2_themes_compare.py
========================
The same Plot, rendered under each bundled theme. It is defined once;
replace() derives a new Plot with a different theme rather than mutating this
one, so the original stays reusable.

Output: plots/plot2_themes_compare_dkit-light.png
        plots/plot2_themes_compare_dkit-dark.png
        plots/plot2_themes_compare_dkit-print.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import Plot, geom, scale


rows = by_month(nottem())
seasons = Plot(
    geom.Bar("Mean", x="month", y="mean"),
    geom.Line("Warmest year", x="month", y="high", color="negative"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Theme comparison",
)

for name in ("dkit-light", "dkit-dark", "dkit-print"):
    filename = f"plots/plot2_themes_compare_{name}.png"
    seasons.replace(theme=name).save(rows, filename, dpi=110)
    print(f"wrote {filename}")
