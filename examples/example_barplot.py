import sys; sys.path.insert(0, "..")  # noqa
from dkit.plot import ggrammar
from dkit.plot.gnuplot import BackendGnuPlot


data = [
    {"index": "jan", "sales": 15, "revenue": 20},
    {"index": "feb", "sales": 10, "revenue": 30},
    {"index": "mar", "sales": 13, "revenue": 25},
    {"index": "apr", "sales": 10, "revenue": 20},
    {"index": "may", "sales": 10, "revenue": 50},
    {"index": "jun", "sales": 10, "revenue": 20},
    {"index": "jul", "sales": 10, "revenue": 20},
]

gg = ggrammar.Plot(data) \
    + ggrammar.GeomBar("Revenue", y_data="revenue", x_data="index", color="#0000FF", alpha=0.8) \
    + ggrammar.Title("2018 Sales") \
    + ggrammar.YAxis("Rand", min=0, max=100, ticks=1) \
    + ggrammar.XAxis("Month") \
    + ggrammar.Aesthetic(stacked=True, width=15, height=10)


BackendGnuPlot(gg.as_dict(), terminal="svg").render(
    file_name="example_barplot.svg",
    script_name="example_barplot.plot"
)
