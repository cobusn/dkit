"""
plot2_data.py
=============
Shared data loading for the plot2_*.py examples.

Not an example itself -- every plot2_*.py script imports from here rather than
repeating the same source.load() and field parsing, so each script stays
about the one plot it demonstrates. Aggregation stays in each script: plot2
draws rows, it does not compute statistics, and which aggregation a plot
needs is part of what the example is showing.
"""
import random
from datetime import date, timedelta

from dkit.data import aggregation as agg
from dkit.etl import source


MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
PORTS = {"C": "Cherbourg", "Q": "Queenstown", "S": "Southampton"}
CLASSES = {1: "First", 2: "Second", 3: "Third"}


def nottem():
    """monthly average air temperature at Nottingham Castle, 1920-1939"""
    with source.load("data/nottem_temp.jsonl") as src:
        return [
            {
                "date": date(int(r["Year"]), MONTHS.index(r["Month"]) + 1, 1),
                "year": int(r["Year"]),
                "month": r["Month"],
                "temp": float(r["Temp"]),
            }
            for r in src
        ]


def titanic():
    """Titanic passengers, by class and port of embarkation"""
    with source.load("data/titanic.csv") as src:
        return [
            {
                "class": CLASSES[int(r["Pclass"])],
                "port": PORTS.get(r["Embarked"], "Unknown"),
                "fare": float(r["Fare"]) if r["Fare"] else 0.0,
            }
            for r in src
        ]


def by_month(monthly):
    """one row per calendar month, in calendar order, with mean/low/high"""
    return list(
        (
            agg.Aggregate()
            + agg.GroupBy("month")
            + agg.Mean("temp").alias("mean")
            + agg.Min("temp").alias("low")
            + agg.Max("temp").alias("high")
        )(monthly)
    )


def by_year(monthly):
    """one row per year, with the mean and a running share of the total"""
    rows = list(
        (agg.Aggregate() + agg.GroupBy("year") + agg.Mean("temp").alias("mean"))(monthly)
    )
    total = sum(r["mean"] for r in rows)
    running = 0.0
    for row in rows:
        running += row["mean"]
        row["share"] = running / total
    return rows


def with_decade(monthly):
    """monthly rows with decade and year_in_decade fields added"""
    return [
        dict(r, decade=f"{r['year'] // 10 * 10}s", year_in_decade=r["year"] % 10)
        for r in monthly
    ]


def by_decade(monthly):
    """one row per month per decade -- the shape a slope plot wants"""
    grouped = list(
        (
            agg.Aggregate()
            + agg.GroupBy("month", "decade")
            + agg.Mean("temp").alias("mean")
        )(with_decade(monthly))
    )
    grouped.sort(key=lambda r: MONTHS.index(r["month"]))
    return grouped


def daily_activity(days=440, seed=1):
    """synthetic daily commit counts, spanning more than a calendar year

    Weekends run quieter than weekdays, which is what the calendar heatmap
    examples are showing: the span is whatever ``days`` covers, not a year.
    """
    rng = random.Random(seed)
    start = date(2023, 9, 1)
    rows = []
    for i in range(days):
        day = start + timedelta(days=i)
        base = 1.5 if day.weekday() >= 5 else 5.0
        rows.append({"date": day, "commits": round(max(0.0, rng.gauss(base, 2.0)))})
    return rows


def daily_net_change(days=440, seed=2):
    """synthetic daily net gain/loss, signed around zero"""
    rng = random.Random(seed)
    start = date(2023, 9, 1)
    rows = []
    for i in range(days):
        day = start + timedelta(days=i)
        rows.append({"date": day, "net": round(rng.gauss(0.0, 50.0), 1)})
    return rows


def titanic_groups(passengers):
    """passengers aggregated by class and port, with a display label"""
    groups = list(
        (
            agg.Aggregate()
            + agg.GroupBy("class", "port")
            + agg.Count("fare").alias("passengers")
            + agg.Mean("fare").alias("mean_fare")
            + agg.Sum("fare").alias("revenue")
        )(passengers)
    )
    for row in groups:
        row["cell"] = f"{row['class']}\n{row['port']}"
        row["group"] = f"{row['class']}, {row['port']}"
    return groups
