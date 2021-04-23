import sys; sys.path.insert(0, "..")  # noqa

from dkit.etl import source
from dkit.plot import ggrammar
from dkit.plot.matplotlib import MPLBackend


with source.load("data/runstats.jsonl.xz") as src:
    data = list(src)

ggrammar = ggrammar.Plot(data) \
    + ggrammar.GeomHeatMap(
        "Performance",
        y="position",
        x="round",
        z="score"
    ) \
    + ggrammar.Title("Genetic Algorithm Performance") \
    + ggrammar.YAxis("Individuals") \
    + ggrammar.XAxis("Generation") \
    + ggrammar.ZAxis("Score") \
    + ggrammar.Aesthetic(width=15, height=10)


MPLBackend().render(
    ggrammar.as_dict(),
    file_name="example_heatmap.pdf"
)
