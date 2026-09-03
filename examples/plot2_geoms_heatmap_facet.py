"""
plot2_geoms_heatmap_facet.py
==============================
Faceting a heat map. The x field is the year *within* its decade, so that
both panels span the same ten columns -- faceting on the calendar year
instead would leave each panel's own decade empty. The colour range has to be
given explicitly: scales are shared across panels but a colour map is a
layer's own business, so each panel would otherwise range over its own data
and the two pictures could not be compared.

Output: plots/plot2_geoms_heatmap_facet.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem, with_decade

from dkit.plot2 import Plot, geom, save_figure, scale


rows = with_decade(nottem())
temps = [r["temp"] for r in rows]

filename = "plots/plot2_geoms_heatmap_facet.png"
fig = Plot(
    geom.HeatMap("Temperature (F)", x="year_in_decade", y="month", z="temp",
                 vmin=min(temps), vmax=max(temps)),
    x=scale.Categorical("Year within the decade"),
    y=scale.Categorical("Month", limits=(11.5, -0.5)),
    title="Nottingham Castle by decade",
).facet(rows, by="decade", ncols=2, panel_width=12.0, panel_height=8.0)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
