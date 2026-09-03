"""
plot2_canned_report.py
========================
The canned analyses in a document. doc.wrap_matplotlib takes any function
returning a figure, so a canned chart goes into a report with no plot
grammar to serialise and no decorator to remember. dkit.doc2.canned tabulates
the same analysis objects used to draw the charts, so a chart and its table
cannot disagree.

Output: plots/plot2_canned_report_pareto.png
        plots/plot2_canned_report.html
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem, titanic, titanic_groups

from dkit.data.boston import BostonMatrix
from dkit.data.pareto import ParetoAnalysis
from dkit.doc2 import canned as tables
from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.plot2 import canned


monthly = nottem()
matrix = BostonMatrix(
    monthly, id_field="month", sequence_field="year", value_field="temp",
    window_size=8,
)

fares = ParetoAnalysis(titanic_groups(titanic()), value_field="revenue",
                        label_field="group")

report_plot = "plots/plot2_canned_report_pareto.png"


@doc.wrap_matplotlib(filename=report_plot)
def fare_chart():
    return canned.pareto(fares, title="Fare revenue by group", rotation=45)


report = doc.Document(
    title="Canned report",
    sub_title="Titanic fares and Nottingham temperatures",
    author="dkit examples",
)
report.add_template("""
## Fare concentration

{{ chart() }}

The table below is the same analysis object that drew the chart, so the
percentages match by construction.
""", chart=fare_chart)
report.add_element(tables.pareto_table(fares, cum_percent=80.0,
                                       label_title="Class and port",
                                       value_title="Fares"))

# BostonTables holds the presentation decisions BostonMatrix does not:
# headings, formats, and how many rows a report shows.
temperature_tables = tables.BostonTables(
    matrix, id_title="Month", value_title="1939", value_format="{:,.1f}F", top_n=6
)
report.add_template("""
## Warmest months

Ranked by the median of the trailing eight year window.
""")
report.add_element(temperature_tables.top_by_value())

report_file = "plots/plot2_canned_report.html"
HtmlRenderer(report, inline_images=True).render(report_file)
print(f"wrote {report_plot}")
print(f"wrote {report_file}")
