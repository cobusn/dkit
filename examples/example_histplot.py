import sys; sys.path.insert(0, "..")  # noqa
from random import gauss

from dkit.data.histogram import Histogram
from dkit.data.stats import Accumulator
from dkit.plot import ggrammar
from dkit.plot.gnuplot import BackendGnuPlot
from dkit.plot.matplotlib import MPLBackend


a = Accumulator((gauss(0.0, 1.0) for i in range(100000)))
data = Histogram.from_accumulator(a, precision=2)

plt = ggrammar.Plot(data) \
    + ggrammar.GeomHistogram("random data", "#FF0000", 0.8) \
    + ggrammar.Title("Random Data Histogram") \
    + ggrammar.YAxis("Frequency") \
    + ggrammar.XAxis("bin", which=1) \
    + ggrammar.Aesthetic(width=15, height=10)


BackendGnuPlot(terminal="svg").render(
    plt.as_dict(),
    file_name="example_hist.svg",
    script_name="example_hist.plot"
)


MPLBackend(terminal="svg").render(
    plt.as_dict(),
    file_name="example_mpl_histogram.svg"
)
