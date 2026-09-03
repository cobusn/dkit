"""
plot2_geoms_heatmap.py
========================
quick.heatmap: a value over two categorical dimensions at once. Twenty years
by twelve months is 240 cells: a line per year would be unreadable, and the
seasonal band is obvious here. A heat map is an ordinary geom -- it draws
inside two Categorical scales, unlike TreeMap or Slope, which own their whole
Axes.

Output: plots/plot2_geoms_heatmap.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import quick, save_figure, scale


nottem_rows = nottem()

filename = "plots/plot2_geoms_heatmap.png"
fig = quick.heatmap(
    nottem_rows, x="year", y="month", z="temp", label="Temperature (F)",
    title="Nottingham Castle, 1920-1939",
    # a discrete domain runs upwards from position zero, which would put
    # January at the bottom; flipping the limits reads the calendar down
    yscale=scale.Categorical("Month", limits=(11.5, -0.5)),
    xscale=scale.Categorical("Year", rotation=90),
    width=18.0, height=10.0,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")
