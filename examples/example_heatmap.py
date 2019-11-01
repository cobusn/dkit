import sys; sys.path.insert(0, "..")  # noqa
from dkit.plot import ggrammar
from dkit.plot.matplotlib import MPLBackend


from dkit.etl import source

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


MPLBackend(ggrammar.as_dict()).render(
    file_name="example_heatmap.pdf"
)
