"""
plot2_quick.py
==============
Demonstrates the dkit.plot2 tier B API: one call per chart.

Where plot2_core.py composes layers and names its scales, this module is the
short way to the common case.  Each function takes rows and field names and
returns a Figure:

    fig = quick.bar(rows, x="month", y="revenue", title="2026 Sales")

The example covers:
  - quick.bar, line, area, scatter and hist
  - scale inference: a date field gets a real time axis, a string field a
    categorical one, a number a linear one
  - overriding the inference with xscale= / yscale=
  - the ax= escape hatch, drawing several quick calls onto one grid
  - Plot.facet, one panel per group with scales shared across panels

quick contains no drawing logic: every function is a signature plus one Plot
expression.  When the layer structure becomes the caller's choice — a target
line, an overlay, a twin axis — use Plot directly, as plot2_core.py does.

Output is written to plots/plot2q_*.png.
"""
import sys; sys.path.insert(0, "..")  # noqa
from datetime import date

import matplotlib.pyplot as plt

from dkit.data import aggregation as agg
from dkit.etl import source
from dkit.plot2 import Plot, geom, get_theme, quick, save_figure, scale


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
OUTPUT = "plots/{}.png"
written = []


def keep(fig, name):
    """save a figure and record the filename for the summary at the end

    save_figure rather than fig.savefig: the style sheets set savefig.bbox, but
    rcParams only apply inside the theme's context, and by the time a quick
    function has returned its figure we are outside it.  save_figure applies
    the theme again for the save, and closes the figure afterwards.
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

by_month = list(
    (
        agg.Aggregate()
        + agg.GroupBy("month")
        + agg.Mean("temp").alias("mean")
        + agg.Max("temp").alias("high")
    )(monthly)
)


# ------------------------------------------------------------------------
# 1. one call, one chart
# ------------------------------------------------------------------------
# No scales named: "month" holds strings so the x axis is categorical, and
# "mean" holds floats so the y axis is linear.
keep(
    quick.bar(by_month, x="month", y="mean", title="Average by month",
              xlabel="Month", ylabel="Temperature (F)"),
    "plot2q_bar",
)

# "date" holds date objects, so this gets a real datetime axis with calendar
# ticks — without the caller asking for one.
keep(
    quick.line(monthly, x="date", y="temp", title="Monthly mean, 1920-1939",
               ylabel="Temperature (F)"),
    "plot2q_line",
)

keep(
    quick.area(by_month, x="month", y="high", title="Warmest year on record",
               xlabel="Month", ylabel="Temperature (F)", color="negative"),
    "plot2q_area",
)

# size= may name a field, giving one marker area per row
keep(
    quick.scatter(by_month, x="mean", y="high", size="mean",
                  title="Mean against maximum", xlabel="Monthly mean (F)",
                  ylabel="Warmest year (F)"),
    "plot2q_scatter",
)

# binning is dkit.data.histogram's job; hist just draws the result
keep(
    quick.hist(monthly, "temp", bins=20, title="Distribution of monthly means",
               xlabel="Temperature (F)"),
    "plot2q_hist",
)


# ------------------------------------------------------------------------
# 2. overriding the inferred scale
# ------------------------------------------------------------------------
# Inference is a default, not a rule.  Pass a scale to say something the data
# cannot: here, that the axis should read as a percentage of the maximum.
peak = max(r["high"] for r in by_month)
for row in by_month:
    row["share"] = row["mean"] / peak

keep(
    quick.bar(by_month, x="month", y="share", title="Share of the record high",
              xscale=scale.Categorical("Month", rotation=45),
              yscale=scale.Percent("Share of maximum", whole=1.0)),
    "plot2q_scales",
)


# ------------------------------------------------------------------------
# 3. ax=: several quick calls on one figure
# ------------------------------------------------------------------------
# Every entry point draws into a supplied Axes, so quick functions compose
# into any matplotlib layout you care to build.
theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(2, 2, figsize=(11, 6))

quick.bar(by_month, x="month", y="mean", ax=axes[0][0], title="bar")
quick.line(monthly, x="date", y="temp", ax=axes[0][1], title="line")
quick.area(by_month, x="month", y="high", ax=axes[1][0], title="area")
quick.hist(monthly, "temp", bins=15, ax=axes[1][1], title="hist")
fig.tight_layout()
keep(fig, "plot2q_grid")


# ------------------------------------------------------------------------
# 4. faceting
# ------------------------------------------------------------------------
# One Plot, one panel per group.  Scales are collected from all the rows, so
# the panels are comparable: every panel shows the same twelve months in the
# same order on the same y range, which is what makes the shape differences
# between decades readable.
seasons = Plot(
    geom.Line("Monthly mean", x="month", y="temp", marker="o", marker_size=3),
    x=scale.Categorical("Month", rotation=90),
    y=scale.Linear("Temperature (F)"),
    title="Nottingham Castle by year",
)
keep(seasons.facet(monthly, by="year", ncols=5), "plot2q_facet")

# a smaller grid, and per-panel y ranges instead of a shared one
keep(
    seasons.replace(title="Four years, independent y ranges").facet(
        [r for r in monthly if r["year"] < 1924],
        by="year", ncols=2, share_y=False, panel_width=8.0,
    ),
    "plot2q_facet_free",
)


print(f"wrote {len(written)} figures:")
for filename in written:
    print(f"  {filename}")
