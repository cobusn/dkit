"""
Canned' report components that assist in getting up to speed
quickly with reports.
"""
import statistics
import string
from functools import lru_cache

from ..data import window as win
from ..data.containers import OrderedSet
from ..data.histogram import binner
from .builder import is_table, is_plot
from .document import Table, Figure
from ..utilities.file_helper import sanitise_name
from .. import CURRENCY_FORMAT


NWDT = 1.5  # width of numeric column

__all__ = [
    "BostonMatrix",
    "histogram_plot",
    "pareto_plot",
]


@is_plot
def histogram_plot(data, value_field, value_heading, entity_name, title=None, bins=10,
                   bin_digits=1, file_name=None, float_format="{:.0f}"):

    h_bins = binner(data, value_field, bins, bin_digits=bin_digits)
    _file_name = file_name or f"{sanitise_name(value_field)}_{sanitise_name(entity_name)}_hist.pdf"
    title_ = title or f"{entity_name} Histogram"

    g = Figure(h_bins, filename=_file_name) \
        + Figure.GeomHistogram(title, alpha=0.5) \
        + Figure.Title(title_) \
        + Figure.XAxis(value_heading, float_format=float_format) \
        + Figure.YAxis("Frequency")
    return g


@is_plot
def pareto_plot(data, value_field, entity_name, y_legend,
                float_format="R{x:,.0f}"):
    """Pareto Plot"""
    filename = f"{sanitise_name(entity_name)}_pareto.pdf"
    title = f"Pareto Chart: {entity_name}" if entity_name else "Pareto chart"
    data = sorted(
        data,
        key=lambda x: x[value_field],
        reverse=True
    )

    g = Figure(data, filename=filename) \
        + Figure.GeomBar(entity_name, x_data=None, y_data=value_field) \
        + Figure.GeomCumulative(f"Cumulative {y_legend}", value_field) \
        + Figure.Title(title) \
        + Figure.XAxis(f"{entity_name}", defeat=True) \
        + Figure.YAxis(y_legend, float_format=float_format)
    return g


class BostonMatrix(object):
    """Helper class to generate boston matrixes and associated plots etc.

    NOTE: this class will sort the data

    Args:
        - data: list of dicts
        - id_field: unique id
        - sequence_field (usually a date or numeric value)
        - value_field: the value tracked
        - description_field describes item
        - entity_name (optional) used for plot titles
        - window_size (time window)
        - value_format (string.format format string) for plot
        - h_id: table heading for id
        - h_sequence: table heading for sequence column
        - h_value: table heading for value column
        - h_description: table heading for description
        - x_title: title for x axis
        - y_title: title for y axis
        - top_n: limit tables to this number of rows
    """
    def __init__(self, data, id_field, sequence_field, value_field,
                 description_field, entity_name=None, window_size=6,
                 table_value_format=CURRENCY_FORMAT, plot_value_format=CURRENCY_FORMAT,
                 h_id=None, h_sequence=None, h_value=None, h_description=None,
                 y_title="Revenue", x_title="Growth per month", top_n=10):
        self.data = list(sorted(data, key=lambda x: x[sequence_field]))
        self.id_field = id_field
        self.field_value = value_field
        self.field_sequence = sequence_field
        self.field_description = description_field
        self.entity_name = entity_name
        self.window_size = window_size
        self.table_value_format = table_value_format
        self.plot_value_format = plot_value_format
        self.alias_ma = f"ma_{self.window_size}"
        self.alias_gr = f"gr_{self.window_size}"
        self.alias_mean = f"mean_{self.window_size}"
        self.h_id = h_id or id_field
        self.h_sequence = h_sequence or sequence_field
        self.y_title = y_title
        self.x_title = x_title
        self.h_value = h_value or value_field
        self.h_description = h_description or description_field
        self.top_n = top_n

    @lru_cache
    def window(self):
        """Add Moving Average and gradient over Window"""
        win_size = self.window_size
        w = win.MovingWindow(win_size).partition_by(self.id_field) \
            + win.Median(self.field_value, na=0).alias(self.alias_ma) \
            + win.Average(self.field_value, na=0).alias(self.alias_mean) \
            + win.Last(self.field_value).alias("last_value")

        w2 = win.MovingWindow(win_size).partition_by(self.id_field)  \
            + win.Gradient(f"ma_{win_size}", na=0).alias(self.alias_gr)

        return list(w2(w(self.data)))

    @lru_cache
    def last_sequence_id(self):
        return OrderedSet([r[self.field_sequence] for r in self.data]).pop()

    @lru_cache
    def median_last_interval(self):
        return statistics.median(i[self.field_value] for i in self.last_interval())

    def last_interval(self):
        last_sequence = self.last_sequence_id()
        r = [
            i for i in self.window()
            if i[self.field_sequence] == last_sequence
        ]
        return r

    def col_description(self, title="Description", width=5, format_="{}", align="l"):
        """helper to add description column"""
        return Table.Field(
            self.field_description,
            title,
            width=width,
            format_=format_,
            align=align
        )

    def col_identifier(self, title="Identifier", width=2, format_="{}", align="r"):
        """helper to add identifier column"""
        return Table.Field(
            self.id_field,
            title,
            width=width,
            format_=format_,
            align=align
        )

    def col_last_value(self, title=None, width=NWDT, format_=CURRENCY_FORMAT, align="r"):
        title = title or str(self.last_sequence_id())
        return Table.Field(
            self.field_value,
            title,
            width=width,
            format_=self.table_value_format,
            align=align
        )

    def col_growth(self, title="Growth(${n})", width=NWDT, format_=CURRENCY_FORMAT, align="r"):
        title = string.Template(title).safe_substitute({"n": self.window_size})
        return Table.Field(
            self.alias_gr,
            title,
            width=width,
            format_=self.table_value_format,
            align=align
        )

    def col_median(self, title="Median(${n})", width=NWDT, format_=CURRENCY_FORMAT, align="r"):
        title = string.Template(title).safe_substitute({"n": self.window_size})
        return Table.Field(
            self.alias_ma,
            title,
            width=width,
            format_=self.table_value_format,
            align=align
        )

    def col_sparkline_values(self, title="History", width=2):
        """Create a sparkline of values"""
        return Table.SparkLine(
            self.window(),
            self.id_field,
            self.id_field,
            self.field_value,
            title=title,
            width=width
        )

    def col_sparkline_ma(self, title="Moving Average", width=2):
        """Create a sparkline of values"""
        return Table.SparkLine(
            self.window(),
            self.id_field,
            self.id_field,
            self.alias_ma,
            title=title,
            width=width
        )

    def data_by_share(self):
        """
        data ordered by share
        """
        return list(
            sorted(
                self.last_interval(),
                key=lambda x: x[self.field_value],
                reverse=True
            )
        )

    def data_by_growth(self):
        """
        data ordered by share
        """
        return list(
            sorted(
                (i for i in self.last_interval() if i[self.alias_gr] >= 0),
                key=lambda x: x[self.alias_gr],
                reverse=True
            )
        )

    def data_by_decline(self):
        """
        data ordered by share
        """
        return list(
            sorted(
                (i for i in self.last_interval() if i[self.alias_gr] < 0),
                key=lambda x: x[self.alias_gr],
                reverse=False
            )
        )

    def data_q1(self):
        """high revenue negative growth"""
        quadrant = "Quadrant 1"

        def add_q(row):
            row["quadrant"] = quadrant
            return row

        return list(map(add_q, sorted(
            (
                i for i in self.last_interval()
                if i[self.alias_ma] > self.median_last_interval()
                and i[self.alias_gr] < 0
            ),
            key=lambda x: x[self.alias_gr]
        )))

    def data_q2(self):
        """high revenue positive growth"""
        quadrant = "Quadrant 2"

        def add_q(row):
            row["quadrant"] = quadrant
            return row

        return list(map(add_q, sorted(
            (
                i for i in self.last_interval()
                if i[self.alias_ma] > self.median_last_interval()
                and i[self.alias_gr] >= 0
            ),
            key=lambda x: x[self.alias_gr],
            reverse=True
        )))

    def data_q3(self):
        """high revenue negative growth"""
        quadrant = "Quadrant 3"

        def add_q(row):
            row["quadrant"] = quadrant
            return row

        return list(map(add_q, sorted(
            (
                i for i in self.last_interval()
                if i[self.alias_ma] < self.median_last_interval()
                and i[self.alias_gr] >= 0
            ),
            key=lambda x: x[self.alias_gr],
            reverse=True
        )))

    def data_q4(self):
        """low revenue negative growth"""
        quadrant = "Quadrant 4"

        def add_q(row):
            row["quadrant"] = quadrant
            return row

        return list(map(add_q, sorted(
            (
                i for i in self.last_interval()
                if i[self.alias_ma] < self.median_last_interval()
                and i[self.alias_gr] < 0
            ),
            key=lambda x: x[self.alias_gr]
        )))

    def table_share(self):
        return self.formatted_table(self.data_by_share()[:self.top_n])

    def table_growth(self):
        return self.formatted_table(self.data_by_growth()[:self.top_n])

    def table_decline(self):
        return self.formatted_table(self.data_by_decline()[:self.top_n])

    def table_q1(self):
        return self.formatted_table(self.data_q1()[:self.top_n])

    def table_q2(self):
        return self.formatted_table(self.data_q2()[:self.top_n])

    def table_q3(self):
        return self.formatted_table(self.data_q3()[:self.top_n])

    def table_q4(self):
        return self.formatted_table(self.data_q4()[:self.top_n])

    @is_plot
    def quadrant_plot(self):
        filename = f"{sanitise_name(self.entity_name)}_quadrants.pdf"
        title = f"{self.entity_name} Quadrants" if self.entity_name else "Quadrant Distribution"

        g = Figure(self.last_interval(), filename=filename) \
            + Figure.GeomScatter("Quadrants", x_data=self.alias_gr, y_data=self.field_value,
                                 alpha=0.5) \
            + Figure.Title(title) \
            + Figure.XAxis(self.x_title, float_format=self.plot_value_format) \
            + Figure.YAxis(self.y_title, float_format=self.plot_value_format) \
            + Figure.HLine(self.median_last_interval(), line_width=0.5, color="black",
                           alpha=0.5) \
            + Figure.VLine(0, line_width=0.5, color="black", alpha=0.5) \
            + Figure.AnchoredText("Q1", location="upper left", size=9, alpha=0.2) \
            + Figure.AnchoredText("Q2", location="upper right", size=9, alpha=0.2) \
            + Figure.AnchoredText("Q3", location="lower right", size=9, alpha=0.2) \
            + Figure.AnchoredText("Q4", location="lower left", size=9, alpha=0.2)

        return g

    def last_interval_histogram_plot(self, bins=10, bin_digits=1, file_name=None):
        return histogram_plot(
            data=self.last_interval(),
            value_field=self.field_value,
            value_heading=self.h_value,
            entity_name=self.entity_name,
            bins=bins,
            bin_digits=bin_digits,
            file_name=file_name
        )

    @is_plot
    def pareto_plot(self):
        """Pareto Plot"""
        filename = f"{sanitise_name(self.entity_name)}_pareto.pdf"
        title = f"Pareto Chart: {self.entity_name}" if self.entity_name else "Pareto plot"
        data = sorted(
            (i for i in self.last_interval() if i["last_value"] > 0),
            key=lambda x: x["last_value"],
            reverse=True
        )
        g = Figure(data, filename=filename) \
            + Figure.GeomBar("Revenue", x_data=None, y_data="last_value") \
            + Figure.GeomCumulative("Cumulative Revenue", "last_value") \
            + Figure.Title(title) \
            + Figure.XAxis(f"{self.entity_name}", defeat=True) \
            + Figure.YAxis(self.y_title, float_format=self.plot_value_format)
        return g

    @is_table
    def formatted_table(self, data):
        if self.field_description:
            fields = [
                self.col_identifier(self.h_id, align="c", width=1.75),
                self.col_description(self.h_description, width=6),
                self.col_sparkline_values("History"),
                self.col_sparkline_ma(f"MA({self.window_size})"),
                self.col_last_value(width=1.75),
                self.col_median(width=1.75),
                self.col_growth(width=1.75),
            ]
        else:
            fields = [
                self.col_identifier(self.h_id, align="l", width=7.75),
                self.col_sparkline_values("History"),
                self.col_sparkline_ma(f"MA({self.window_size})"),
                self.col_last_value(width=1.75),
                self.col_median(width=1.75),
                self.col_growth(width=1.75),
            ]

        t = Table(
            data,
            fields,
            align="l"
        )
        return t
