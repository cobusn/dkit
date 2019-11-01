import sys; sys.path.insert(0, "..")  # noqa
from dkit.plot import ggrammar
from dkit.plot.gnuplot import BackendGnuPlot
from example_barplot import data

ggrammar = ggrammar.Plot(data) \
    + ggrammar.Title("2018 Sales") \
    + ggrammar.GeomBar("Revenue", y_data="revenue", x_data="index", color="#FF0000", alpha=0.8) \
    + ggrammar._Axis("Rand", min=0, max=100, ticks=1) \
    + ggrammar._Axis("Month", which=1)

print(BackendGnuPlot(ggrammar.as_dict()).render_str())
