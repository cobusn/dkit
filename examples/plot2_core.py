"""
plot2_core.py
=============
Demonstrates dkit.plot2, the matplotlib plotting toolkit.

A plot is a declarative value: layers plus scales plus a theme.  Nothing is
drawn until ``render`` or ``save`` is called, so the same ``Plot`` can be
reused against different data.

The example covers:
  - geom.Line, Area, Band, Stem, Bar, Scatter, HLine, VLine and Text
  - scale.Time for a genuine datetime axis, scale.Categorical, scale.Linear
    and scale.Percent
  - the twin right-hand axis via axis="right"
  - per-layer filtering with where=
  - semantic colours ("positive", "negative", "neutral") and color="signed"
  - the bundled themes, and deriving a custom one with Theme.replace
  - register_theme and set_default_theme, for reaching a theme by name and
    for not naming it on every plot

Aggregation is done by dkit.data: plot2 draws rows, it does not compute
statistics.

Output is written to plots/plot2_*.png.
"""
import sys; sys.path.insert(0, "..")  # noqa
from datetime import date

from dkit.data import aggregation as agg
from dkit.etl import source
from dkit.plot2 import (
    Plot, geom, get_theme, register_theme, scale, set_default_theme
)


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
OUTPUT = "plots/{}.png"
written = []


def save(plot, data, name):
    """save a plot and record the filename for the summary at the end"""
    filename = OUTPUT.format(name)
    plot.save(data, filename, dpi=110)
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

# one row per calendar month, in calendar order
by_month = list(
    (
        agg.Aggregate()
        + agg.GroupBy("month")
        + agg.Mean("temp").alias("mean")
        + agg.Min("temp").alias("low")
        + agg.Max("temp").alias("high")
    )(monthly)
)
overall_mean = sum(r["temp"] for r in monthly) / len(monthly)
for row in by_month:
    row["variance"] = row["mean"] - overall_mean

# one row per year, with a running share of the twenty-year total
by_year = list(
    (agg.Aggregate() + agg.GroupBy("year") + agg.Mean("temp").alias("mean"))(monthly)
)
total = sum(r["mean"] for r in by_year)
running = 0.0
for row in by_year:
    running += row["mean"]
    row["share"] = running / total


# ------------------------------------------------------------------------
# 1. a real datetime axis
# ------------------------------------------------------------------------
# scale.Time hands the axis to matplotlib's date locator, so ticks land on
# real calendar boundaries rather than on row numbers.
save(
    Plot(
        geom.Line("Monthly mean", x="date", y="temp"),
        geom.HLine(overall_mean, label="20 year mean"),
        x=scale.Time("Year"),
        y=scale.Linear("Temperature (F)"),
        title="Nottingham Castle, 1920-1939",
    ),
    monthly,
    "plot2_time",
)


# ------------------------------------------------------------------------
# 2. layering: a band, a line and a filtered scatter
# ------------------------------------------------------------------------
# Each layer reads the same rows.  ``where`` filters one layer only, which is
# how the outlying months get their own colour without a second data set.
save(
    Plot(
        geom.Band("Observed range", x="month", lower="low", upper="high",
                  color="neutral"),
        geom.Line("Mean", x="month", y="mean", marker="o"),
        geom.Scatter("Above 60F", x="month", y="mean", color="negative",
                     size=70, where="${mean} > 60"),
        geom.Text("shaded: monthly min to max", location="upper left"),
        x=scale.Categorical("Month"),
        y=scale.Linear("Temperature (F)"),
        title="Seasonal range",
    ),
    by_month,
    "plot2_band",
)


# ------------------------------------------------------------------------
# 3. bars, and colouring by sign
# ------------------------------------------------------------------------
# color="signed" picks the theme's positive, negative or neutral colour per
# row.  In dkit.plot this needed two overlapping bar series.
save(
    Plot(
        geom.Bar("Variance", x="month", y="variance", color="signed"),
        geom.HLine(0.0),
        x=scale.Categorical("Month"),
        y=scale.Linear("Deviation from the 20 year mean (F)"),
        title="Which months run warm",
    ),
    by_month,
    "plot2_signed",
)


# ------------------------------------------------------------------------
# 4. grouped bars, and stacking on a caller-supplied base
# ------------------------------------------------------------------------
# Grouping is two layers at half width, shifted by ``offset``; stacking is a
# layer whose ``base`` names a field holding the running total.  Neither is a
# mode on the geom, so anything you can compute you can stack.
save(
    Plot(
        geom.Bar("Coldest year", x="month", y="low", width=0.4, offset=-0.2),
        geom.Bar("Warmest year", x="month", y="high", width=0.4, offset=0.2),
        x=scale.Categorical("Month"),
        y=scale.Linear("Temperature (F)"),
        title="Grouped bars",
    ),
    by_month,
    "plot2_grouped",
)


# ------------------------------------------------------------------------
# 5. a twin right-hand axis
# ------------------------------------------------------------------------
# axis="right" puts a layer on a second y axis with its own scale.  Any geom
# can use it, and the legend still collects both axes.
save(
    Plot(
        geom.Bar("Annual mean", x="year", y="mean"),
        geom.Line("Cumulative share", x="year", y="share", axis="right",
                  color="highlight", marker="o", marker_size=3),
        x=scale.Categorical("Year", rotation=90),
        y=scale.Linear("Temperature (F)"),
        y_right=scale.Percent("Share of the period total", whole=1.0),
        title="Two scales on one plot",
    ),
    by_year,
    "plot2_twin",
)


# ------------------------------------------------------------------------
# 6. area and stem, with reference lines
# ------------------------------------------------------------------------
save(
    Plot(
        geom.Area("Warmest year", x="month", y="high"),
        geom.Stem("Coldest year", x="month", y="low", color="negative",
                  baseline=True),
        geom.VLine(5.5, label="mid year"),
        x=scale.Categorical("Month"),
        y=scale.Linear("Temperature (F)"),
        title="Area and stem",
    ),
    by_month,
    "plot2_area",
)


# ------------------------------------------------------------------------
# 7. horizontal bars
# ------------------------------------------------------------------------
# x is always the horizontal field and y the vertical one, whichever way the
# bars point, so a horizontal bar chart is a Linear x and a Categorical y.
save(
    Plot(
        geom.Bar("Mean", x="mean", y="month", horizontal=True),
        geom.VLine(overall_mean, label="20 year mean"),
        x=scale.Linear("Temperature (F)"),
        y=scale.Categorical("Month"),
        title="Horizontal bars",
    ),
    by_month,
    "plot2_horizontal",
)


# ------------------------------------------------------------------------
# 8. the same plot under every theme
# ------------------------------------------------------------------------
# The plot is defined once.  ``replace`` derives a new Plot with a different
# theme rather than mutating this one, so the original stays reusable.
seasons = Plot(
    geom.Bar("Mean", x="month", y="mean"),
    geom.Line("Warmest year", x="month", y="high", color="negative"),
    x=scale.Categorical("Month"),
    y=scale.Linear("Temperature (F)"),
    title="Theme comparison",
)

for name in ("dkit-light", "dkit-dark", "dkit-print"):
    save(seasons.replace(theme=name), by_month, f"plot2_theme_{name}")

# a custom theme derived from a bundled one: only what differs is stated
corporate = get_theme("dkit-light").replace(
    positive="#1b7f4b",
    negative="#a4243b",
    neutral="#8d99ae",
    number_format="{x:,.1f}",
)
save(seasons.replace(theme=corporate, title="Custom theme"), by_month,
     "plot2_theme_custom")

# Registering it buys one thing: reaching it by *name*, from a config file, a
# command line argument, or a plot written before the theme existed.  Making it
# the default buys another: a Plot that names no theme now uses it, so a script
# states its house style once instead of on every plot.
register_theme("corporate", corporate)
set_default_theme("corporate")
save(seasons.replace(title="Default theme, not named on the plot"), by_month,
     "plot2_theme_default")
set_default_theme(None)


print(f"wrote {len(written)} figures:")
for filename in written:
    print(f"  {filename}")
