"""
plot2_special.py
================
Demonstrates the dkit.plot2 specialised plots: treemap, heat map and slope.

These three answer questions the ordinary geoms cannot.  A treemap shows how a
total divides up when there are too many parts for a bar chart.  A heat map
shows a value over two categorical dimensions at once.  A slope plot shows
what moved between two points in time, and by how much, for many series at
once.

Two of them -- the treemap and the slope plot -- own their whole Axes rather
than adding marks to a pair of scales, so they come in two forms:

  - a standalone class in dkit.plot2.matplotlib_extra, built once and drawn
    repeatedly.  Colours it has assigned persist between draws, which is what
    makes a set of plots readable side by side.
  - a geom wrapping that class, so a Plot can give it a theme, a title, a
    figure size, save() and facet().

The heat map is an ordinary geom: it draws inside two Categorical scales.

The example covers:
  - quick.treemap, quick.heatmap and quick.slope
  - geom.TreeMap, geom.HeatMap and geom.Slope inside a Plot
  - matplotlib_extra.TreeMap and SlopePlot used directly, sharing colours
    across draws
  - sequential colouring with norm= and a shared color_range
  - faceting a treemap, with region colours stable across the panels
  - the Axes-owning ("exclusive") layers refusing to share a plot

Aggregation is done by dkit.data: plot2 draws rows, it does not compute
statistics.

Output is written to plots/plot2s_*.png.
"""
import sys; sys.path.insert(0, "..")  # noqa
import matplotlib.pyplot as plt

from dkit.data import aggregation as agg
from dkit.etl import source
from dkit.exceptions import DKitPlotException
from dkit.plot2 import Plot, geom, get_theme, quick, save_figure, scale
from dkit.plot2.matplotlib_extra import SlopePlot, TreeMap


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
PORTS = {"C": "Cherbourg", "Q": "Queenstown", "S": "Southampton"}
CLASSES = {1: "First", 2: "Second", 3: "Third"}
OUTPUT = "plots/{}.png"
written = []


def keep(fig, name):
    """save a figure and record the filename for the summary at the end

    save_figure rather than fig.savefig: the style sheets set savefig.bbox, but
    rcParams only apply inside the theme's context, and a figure handed back
    from quick or from a standalone class arrives outside it.  Plot.save does
    not need this because it saves within the context.
    """
    filename = OUTPUT.format(name)
    save_figure(fig, filename, dpi=110)
    written.append(filename)


def save(plot, data, name, **kwargs):
    """save a Plot and record the filename"""
    filename = OUTPUT.format(name)
    plot.save(data, filename, dpi=110, **kwargs)
    written.append(filename)


# ------------------------------------------------------------------------
# data: monthly average air temperature at Nottingham Castle, 1920-1939
# ------------------------------------------------------------------------
with source.load("data/nottem_temp.jsonl") as src:
    nottem = [
        {
            "year": int(r["Year"]),
            "month": r["Month"],
            "decade": f"{int(r['Year']) // 10 * 10}s",
            "year_in_decade": int(r["Year"]) % 10,
            "temp": float(r["Temp"]),
        }
        for r in src
    ]

# one row per month per decade: the shape a slope plot wants, one line per
# month running across the two decades
by_decade = list(
    (
        agg.Aggregate()
        + agg.GroupBy("month", "decade")
        + agg.Mean("temp").alias("mean")
    )(nottem)
)
# calendar order, not the order the file happens to be in
by_decade.sort(key=lambda r: MONTHS.index(r["month"]))

# ------------------------------------------------------------------------
# data: Titanic passengers, by class and port of embarkation
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
        + agg.Count("fare").alias("passengers")
        + agg.Mean("fare").alias("mean_fare")
    )(passengers)
)
for row in groups:
    row["cell"] = f"{row['class']}\n{row['port']}"


# ------------------------------------------------------------------------
# 1. the quick calls
# ------------------------------------------------------------------------
# One rectangle per row, its area proportional to the passenger count.  The
# cells are coloured by class, so the three blocks read as blocks even though
# squarify lays them out for aspect ratio rather than by group.
keep(
    quick.treemap(
        groups, "cell", "passengers", color_field="class",
        value_format="{:,.0f}", title="Titanic passengers by class and port",
        width=16.0, height=9.0,
    ),
    "plot2s_treemap",
)

# A value over two categorical dimensions.  Twenty years by twelve months is
# 240 cells: a line per year would be unreadable, and the seasonal band is
# obvious here.
keep(
    quick.heatmap(
        nottem, x="year", y="month", z="temp", label="Temperature (F)",
        title="Nottingham Castle, 1920-1939",
        # a discrete domain runs upwards from position zero, which would put
        # January at the bottom; flipping the limits reads the calendar down
        yscale=scale.Categorical("Month", limits=(11.5, -0.5)),
        xscale=scale.Categorical("Year", rotation=90),
        width=18.0, height=10.0,
    ),
    "plot2s_heatmap",
)

# What moved, and by how much.  Every month warmed, but not equally: the
# crossing lines are the point of the chart.
keep(
    quick.slope(
        by_decade, "month", "decade", "mean", value_format="{:,.1f}",
        title="Mean temperature by month, 1920s vs 1930s",
        ylabel="Temperature (F)", width=14.0, height=16.0,
    ),
    "plot2s_slope",
)


# ------------------------------------------------------------------------
# 2. sequential colouring
# ------------------------------------------------------------------------
# norm= switches the treemap from one colour per category to a colour ramp
# over the value being plotted, with a colour bar to read it by.  Size and
# colour then say the same thing, which is only worth doing when the ranking
# is the message.
keep(
    quick.treemap(
        groups, "cell", "passengers", norm="linear", value_format="{:,.0f}",
        title="Passengers, shaded by count", width=16.0, height=9.0,
    ),
    "plot2s_treemap_seq",
)

# norm="log" for the same data.  The group sizes here span 1 to 142, and on a
# linear ramp everything below about thirty is the same dark colour; the log
# ramp separates the small groups, at the cost of overstating their differences.
tm = TreeMap(norm="log", value_format="{:,.0f}", figsize=(16.0, 9.0))
fig, ax = tm.draw(groups, "cell", "passengers",
                  title="Passengers, shaded on a log ramp")
keep(fig, "plot2s_treemap_log")


# ------------------------------------------------------------------------
# 3. the standalone classes, drawn more than once
# ------------------------------------------------------------------------
# The reason these classes exist as objects rather than functions: an instance
# remembers which colour it gave each category, so the same port is the same
# colour in both panels even though the second holds a subset of the first.
# A shared color_range does the same job for the sequential case.
by_port = list(
    (
        agg.Aggregate()
        + agg.GroupBy("port")
        + agg.Count("fare").alias("passengers")
    )(passengers)
)
# a copy, not a view: the rows in groups are used again below
third = [dict(r) for r in groups if r["class"] == "Third"]

ports = TreeMap(value_format="{:,.0f}")
theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
ports.draw(by_port, "port", "passengers", title="All passengers",
           theme=theme, ax=axes[0])
ports.draw(third, "port", "passengers", title="Third class only",
           theme=theme, ax=axes[1])
fig.suptitle("One instance, two draws: Cherbourg keeps its colour")
keep(fig, "plot2s_treemap_shared")

# A sequential treemap has no categories to remember, so the same job is done
# by handing both draws one color_range.  Without it each panel would range
# over its own data and the same colour would mean two different counts.
counts = [r["passengers"] for r in groups]
shaded = TreeMap(norm="linear", value_format="{:,.0f}")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for ax, name in ((axes[0], "First"), (axes[1], "Third")):
    shaded.draw([r for r in groups if r["class"] == name], "port", "passengers",
                title=f"{name} class", color_range=(min(counts), max(counts)),
                theme=theme, ax=ax)
fig.suptitle("One color_range, two draws: the colour bars agree")
keep(fig, "plot2s_treemap_range")

# The slope plot the same way: the second panel zooms in on the second half of
# the year, and because one instance drew both, each month keeps the colour it
# had in the crowded overview.  Note what this depends on -- the wider set is
# drawn first.  Colours are assigned in order of first appearance, so drawing
# the subset first would give it colours one through six and shift every other
# month in the overview.
sp = SlopePlot(value_format="{:,.1f}")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 9))
for ax, months, label in ((axes[0], MONTHS, "All twelve months"),
                          (axes[1], MONTHS[6:], "Jul-Dec")):
    sp.draw([r for r in by_decade if r["month"] in months],
            "month", "decade", "mean", title=label,
            y_label="Temperature (F)", theme=theme, ax=ax)
fig.suptitle("Mean temperature by month, 1920s vs 1930s")
keep(fig, "plot2s_slope_shared")


# ------------------------------------------------------------------------
# 4. inside a Plot
# ------------------------------------------------------------------------
# The geoms buy what the standalone classes do not have: a theme resolved by
# name, a title, a figure size in centimetres, where=, save() and facet().
# Everything else is passed straight through to the wrapped class.
save(
    Plot(
        geom.TreeMap("cell", "passengers", "class", value_format="{:,.0f}"),
        title="Titanic passengers (dkit-dark)",
        theme="dkit-dark", width=16.0, height=9.0,
    ),
    groups, "plot2s_treemap_dark",
)

# A heat map is an ordinary geom, so it composes: annotate=True writes each
# value into its cell in whichever of black or white is readable there.  The
# where= drops the one group holding a single passenger, whose mean is not
# worth reading; the cell is left blank rather than drawn as zero, because a
# gap and a real zero are different facts.
save(
    Plot(
        geom.HeatMap("Mean fare", x="port", y="class", z="mean_fare",
                     annotate=True, format="{:,.0f}", where="${passengers} > 1"),
        x=scale.Categorical("Port of embarkation"),
        y=scale.Categorical("Class"),
        title="Mean fare paid", width=14.0, height=8.0,
    ),
    groups, "plot2s_heatmap_annotated",
)

# geom.Slope takes its value-axis label from the Plot's y scale, which is the
# one part of the ordinary scale machinery an exclusive layer still uses.
save(
    Plot(
        geom.Slope("month", "decade", "mean", value_format="{:,.1f}"),
        y=scale.Linear("Temperature (F)"),
        title="Warming by month", width=12.0, height=16.0,
    ),
    by_decade, "plot2s_slope_plot",
)


# ------------------------------------------------------------------------
# 5. faceting
# ------------------------------------------------------------------------
# One Plot, one panel per class.  The layer holds a single wrapped TreeMap
# instance which every panel draws through, so a port keeps its colour across
# the panels -- exactly what makes the three panels comparable.
keep(
    Plot(
        geom.TreeMap("port", "passengers", "port", value_format="{:,.0f}"),
        title="Passengers by port, per class",
    ).facet(groups, by="class", ncols=3, panel_width=9.0, panel_height=7.0),
    "plot2s_treemap_facet",
)

# The same for a heat map.  The x field is the year *within* its decade, so
# that both panels span the same ten columns; faceting on the calendar year
# instead would leave each panel's own decade empty.  The colour range has to
# be given explicitly: scales are shared across panels but a colour map is a
# layer's own business, so each panel would otherwise range over its own data
# and the two pictures could not be compared.
temps = [r["temp"] for r in nottem]
keep(
    Plot(
        geom.HeatMap("Temperature (F)", x="year_in_decade", y="month", z="temp",
                     vmin=min(temps), vmax=max(temps)),
        x=scale.Categorical("Year within the decade"),
        y=scale.Categorical("Month", limits=(11.5, -0.5)),
        title="Nottingham Castle by decade",
    ).facet(nottem, by="decade", ncols=2, panel_width=12.0, panel_height=8.0),
    "plot2s_heatmap_facet",
)


# ------------------------------------------------------------------------
# 6. what an exclusive layer cannot do
# ------------------------------------------------------------------------
# A treemap or a slope plot owns its whole Axes: it has no pair of scales for
# another layer to share, so mixing is refused at construction time rather
# than producing a picture with two things drawn over each other.
try:
    Plot(
        geom.TreeMap("cell", "passengers"),
        geom.Line("Fare", x="cell", y="mean_fare"),
    )
except DKitPlotException as e:
    print(f"as expected: {e}\n")


print(f"wrote {len(written)} figures:")
for filename in written:
    print(f"  {filename}")
