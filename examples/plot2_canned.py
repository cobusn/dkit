"""
plot2_canned.py
===============
Demonstrates dkit.plot2.canned, the named analyses, and the split that makes
them reusable.

Each canned chart is one analysis drawn the way that analysis is read:

  - canned.control_chart: a band, a centre line, and the breaches marked
  - canned.histogram: quick.hist plus a reference line at the mean
  - canned.pareto: ranked bars with the cumulative share on a second axis
  - canned.quadrant: a scatter cut into four by two reference lines

The arithmetic is *not* here and not in plot2.  dkit.data.pareto.ParetoAnalysis
and dkit.data.boston.BostonMatrix compute the numbers with no matplotlib and no
document imports, dkit.plot2.canned draws them, and dkit.doc2.canned tabulates
the same objects -- so a chart and its table cannot disagree, and an analysis
script pays for neither.

The last section builds a small HTML report from exactly that: one figure
through doc2's wrap_matplotlib, and two tables from the analyses already used
above.

Output is written to plots/plot2c_*.png and plots/plot2c_report.html.
"""
import sys; sys.path.insert(0, "..")  # noqa
import statistics
from datetime import date

from dkit.data import aggregation as agg
from dkit.data.boston import BostonMatrix
from dkit.data.pareto import ParetoAnalysis
from dkit.doc2 import canned as tables
from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.etl import source
from dkit.plot2 import canned, save_figure, scale


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
PORTS = {"C": "Cherbourg", "Q": "Queenstown", "S": "Southampton"}
CLASSES = {1: "First", 2: "Second", 3: "Third"}
OUTPUT = "plots/{}.png"
written = []


def keep(fig, name):
    """save a figure and record the filename for the summary at the end

    save_figure applies the theme's ``savefig.*`` rcParams, which a bare
    ``fig.savefig`` on a returned figure would miss.
    """
    filename = OUTPUT.format(name)
    save_figure(fig, filename, dpi=110)
    written.append(filename)


# ------------------------------------------------------------------------
# data: monthly average air temperature at Nottingham Castle, 1920-1939
# ------------------------------------------------------------------------
with source.load("data/nottem_temp.jsonl") as src:
    monthly = [
        {
            "date": date(int(r["Year"]), MONTHS.index(r["Month"]) + 1, 1),
            "year": int(r["Year"]),
            "month": r["Month"],
            "temp": float(r["Temp"]),
        }
        for r in src
    ]

# ------------------------------------------------------------------------
# 1. a control chart
# ------------------------------------------------------------------------
# The limits are fields on the rows, not something the chart computes: which
# limits are right is a decision about the process.  Here it is two standard
# deviations either side of the twenty year mean, which for weather is a
# statement about an unusual month rather than about a machine going out of
# tolerance -- the chart reads the same either way.
temps = [row["temp"] for row in monthly]
mean, sigma = statistics.mean(temps), statistics.stdev(temps)
observed = [
    dict(row, expected=mean, ucl=mean + 2 * sigma, lcl=mean - 2 * sigma)
    for row in monthly
    if row["year"] < 1925
]

keep(
    canned.control_chart(
        observed, x="date", y="temp", label="Monthly mean",
        breach_label="Unusual month", limits_label="Two sigma",
        expected_label="20 year mean",
        title="Nottingham Castle, 1920-1924",
        xlabel="Month", ylabel="Temperature (F)",
        xscale=scale.Time("Month", format="%b %Y"),
    ),
    "plot2c_control_chart",
)


# ------------------------------------------------------------------------
# 2. a histogram with its mean marked
# ------------------------------------------------------------------------
# quick.hist draws the bars; canned.histogram adds the line a report wants,
# which is the whole difference between the two surfaces.
keep(
    canned.histogram(
        monthly, "temp", bins=15, label="Months", mean_label="20 year mean",
        title="How often a month is warm", xlabel="Temperature (F)",
    ),
    "plot2c_histogram",
)


# ------------------------------------------------------------------------
# data: Titanic fares, aggregated by class and port of embarkation
# ------------------------------------------------------------------------
with source.load("data/titanic.csv") as src:
    passengers = [
        {
            "class": CLASSES[int(r["Pclass"])],
            "port": PORTS.get(r["Embarked"], "Unknown"),
            "fare": float(r["Fare"]) if r["Fare"] else 0.0,
        }
        for r in src
    ]

groups = list(
    (
        agg.Aggregate()
        + agg.GroupBy("class", "port")
        + agg.Sum("fare").alias("revenue")
        + agg.Count("fare").alias("passengers")
    )(passengers)
)
for row in groups:
    row["group"] = f"{row['class']}, {row['port']}"


# ------------------------------------------------------------------------
# 3. a pareto chart
# ------------------------------------------------------------------------
# Build the analysis once and hand it to the chart.  The same object goes to
# doc2's pareto_table further down, so the bars and the table cannot disagree
# about what 80% means.
fares = ParetoAnalysis(groups, value_field="revenue", label_field="group")

keep(
    canned.pareto(
        fares, label="Fares", cumulative_label="Cumulative share",
        title="Where the fare revenue came from", ylabel="Fares paid",
        rotation=45,
    ),
    "plot2c_pareto",
)

vital_few = fares.top_n_percent(80.0)
print(f"{len(vital_few)} of {len(fares)} groups are 80% of the revenue:")
for row in vital_few:
    print(f"  {row['group']:<24} {row['cum_percent']:5.1f}%")

# top= limits the bars without touching the arithmetic: the last bar still
# reports its share of the whole, not of what is drawn.
keep(
    canned.pareto(fares, top=4, title="The same analysis, top four only",
                  ylabel="Fares paid", rotation=45),
    "plot2c_pareto_top",
)


# ------------------------------------------------------------------------
# 4. a boston matrix and its quadrant chart
# ------------------------------------------------------------------------
# One row per entity per period is all BostonMatrix needs.  Here the entity is
# a calendar month and the period a year, so "growth" is whether that month has
# been warming over the trailing window.
matrix = BostonMatrix(
    monthly, id_field="month", sequence_field="year", value_field="temp",
    window_size=8,
)

# The cuts are passed in rather than derived from the plotted rows, so a
# filtered chart still divides where the whole population divides.
keep(
    canned.quadrant(
        matrix.classified, x=matrix.alias_growth, y=matrix.alias_median,
        y_center=matrix.median, label="Months",
        title=f"Warm and warming, {matrix.last_sequence}",
        xlabel="Warming per year (F)", ylabel="Median temperature (F)",
    ),
    "plot2c_quadrant",
)

for n in (1, 2, 3, 4):
    names = [row["month"] for row in matrix.quadrant(n)]
    print(f"Q{n}: {', '.join(names) or '-'}")


# ------------------------------------------------------------------------
# 5. the same analyses in a document
# ------------------------------------------------------------------------
# wrap_matplotlib takes any function returning a figure, so a canned chart goes
# into a report with no plot grammar to serialise and no decorator to remember.
report_plot = OUTPUT.format("plot2c_report_pareto")


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

# BostonTables holds the presentation decisions BostonMatrix does not: headings,
# formats, and how many rows a report shows.
temperature_tables = tables.BostonTables(
    matrix, id_title="Month", value_title="1939", value_format="{:,.1f}F", top_n=6
)
report.add_template("""
## Warmest months

Ranked by the median of the trailing eight year window.
""")
report.add_element(temperature_tables.top_by_value())

report_file = "plots/plot2c_report.html"
HtmlRenderer(report, inline_images=True).render(report_file)
written.append(report_plot)
written.append(report_file)


print(f"wrote {len(written)} files:")
for filename in written:
    print(f"  {filename}")
