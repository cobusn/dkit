#
# Example document with table and plot
#
import sys; sys.path.insert(0, "..") # noqa

from dkit.doc import document
from dkit.doc.document import Table
from dkit.doc.latex_renderer import LatexDocRenderer
from dkit.etl import source
from dkit.data import aggregation as agg

with source.load("data/nottem_temp.jsonl") as src:
    temp_data = list(src)

avg_by_month = agg.Aggregate() \
    + agg.GroupBy('Month') \
    + agg.Mean("Temp").alias("mean_temp") \

d = document.Document()
d += document.Title("Document")
d += document.SubTitle("Temperatures")
d += document.Author("Cobus Nel")
d += document.Heading("Temperature table", 1)
d += document.Table(
    list(avg_by_month(temp_data)),
    [
        Table.Field("Month", width=2),
        Table.Field("mean_temp", "Temp", width=2, align="right"),
        Table.SparkLine(temp_data, "Month", "Month", "Temp", "Plot", 1),
        Table.Field("mean_temp", "Temp 2", width=2, format_="{:.2f}", align="left"),
    ]
)

with open("tex/sparktable.tex", "w") as texfile:
    for st in LatexDocRenderer(d.as_dict()["elements"], plot_folder="plots"):
            texfile.write(st)
