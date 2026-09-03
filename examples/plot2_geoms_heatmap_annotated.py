"""
plot2_geoms_heatmap_annotated.py
==================================
geom.HeatMap inside a Plot composes with the ordinary machinery: annotate=True
writes each value into its cell in whichever of black or white is readable
there. where= drops the one group holding a single passenger, whose mean is
not worth reading -- the cell is left blank rather than drawn as zero,
because a gap and a real zero are different facts.

Output: plots/plot2_geoms_heatmap_annotated.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.plot2 import Plot, geom, scale


groups = titanic_groups(titanic())

filename = "plots/plot2_geoms_heatmap_annotated.png"
Plot(
    geom.HeatMap("Mean fare", x="port", y="class", z="mean_fare",
                 annotate=True, format="{:,.0f}", where="${passengers} > 1"),
    x=scale.Categorical("Port of embarkation"),
    y=scale.Categorical("Class"),
    title="Mean fare paid", width=14.0, height=8.0,
).save(groups, filename, dpi=110)
print(f"wrote {filename}")
