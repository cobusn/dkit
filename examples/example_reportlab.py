import sys
sys.path.insert(0, "..")  # noqa
from reportlab.lib.styles import getSampleStyleSheet
from dkit.doc.reportlab_renderer import ReportLabBuilder, ReportlabDocRenderer
from dkit.utilities.file_helper import yaml_load
from dkit.doc import document
from example_barplot import gg
from dkit.plot import ggrammar


d = document.Document()
d += document.Title("Example document")
d += document.Heading("Heading 1", 1)
d += document.Heading("Heading 2", 2)
d += document.Paragraph("Test para")
d += document.Paragraph("Another test para")
d += document.MD(
    """
    ##  heading 2
    This is **bold** and *italic* text.
    """
)
d += document.LineBreak(0.5)
d += document.Image("plots/forecast.png", "Forecast", width=17, height=8)

l1 = document.List()
l1.add_entry("one")
l1.add_entry("two")
l2 = document.List()
l2.add_entry("next one")
l2.add_entry("next two")
l1.add_entry(l2)
d += l1

d += document.Heading("Figure", 1)

data = [
    {"index": "jan", "sales": 15, "revenue": 20},
    {"index": "feb", "sales": 10, "revenue": 30},
    {"index": "mar", "sales": 13, "revenue": 25},
    {"index": "apr", "sales": 10, "revenue": 20},
    {"index": "may", "sales": 10, "revenue": 50},
    {"index": "jun", "sales": 10, "revenue": 20},
    {"index": "jul", "sales": 10, "revenue": 20},
]

fig = document.Figure(data) \
    + ggrammar.GeomBar("Revenue", y_data="revenue", x_data="index", color="#0000FF", alpha=0.8) \
    + ggrammar.Title("2018 Sales") \
    + ggrammar.YAxis("Rand", min=0, max=100, ticks=1) \
    + ggrammar.XAxis("Month") \
    + ggrammar.Aesthetic(stacked=True, width=15, height=5)

d += fig

with open("stylesheet.yaml") as infile:
    plot_style = yaml_load(infile)
    print(plot_style)
r = ReportlabDocRenderer(d, getSampleStyleSheet(), plot_style)
b = ReportLabBuilder()
content = list(r)
b.run("reportlab_render.pdf", content)
