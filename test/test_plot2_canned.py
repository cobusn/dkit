# Copyright (c) 2026 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""
canned recipes: the analysis in dkit.data, the chart in plot2, the table in doc2

The split is what most of these tests are about: the calculation modules must
stay importable without matplotlib, and the chart, the table and the analysis
must agree on the same numbers.
"""
import sys; sys.path.insert(0, "..")  # noqa
import subprocess
import tempfile
from datetime import date
from os import path
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                                   # noqa: E402
from matplotlib.collections import PathCollection                 # noqa: E402
from matplotlib.figure import Figure                              # noqa: E402

from dkit.data.boston import QUADRANTS, BostonMatrix              # noqa: E402
from dkit.data.pareto import ParetoAnalysis                       # noqa: E402
from dkit.doc2 import canned as doc_canned                        # noqa: E402
from dkit.doc2 import document as doc                             # noqa: E402
from dkit.doc2.html_renderer import HtmlRenderer                  # noqa: E402
from dkit.exceptions import DKitDataException                      # noqa: E402
from dkit.plot2 import canned                                      # noqa: E402


#
# fixtures
#
#: sales against control limits, with one row over and one under
CONTROL = [
    {
        "month": date(2026, i, 1),
        "sales": value,
        "expected": 100.0,
        "ucl": 120.0,
        "lcl": 80.0,
    }
    for i, value in enumerate(
        [100, 104, 96, 130, 108, 92, 100, 110, 70, 105, 99, 101], start=1
    )
]

#: pre-aggregated revenue per customer: the top two are 75% of the total
REVENUE = [
    {"customer": "Alpha", "revenue": 500.0},
    {"customer": "Beta", "revenue": 250.0},
    {"customer": "Gamma", "revenue": 150.0},
    {"customer": "Delta", "revenue": 75.0},
    {"customer": "Epsilon", "revenue": 25.0},
]

#: eighteen months of revenue for four entities, one per quadrant.  Long enough
#: that the last gradient window sees only full medians: a six month window over
#: an eight month series differentiates the ramp-up, not the trend.
PERIODS = 18
TREND = {
    "rising-big": lambda i: 100 + 5 * i,
    "falling-big": lambda i: 200 - 5 * i,
    "rising-small": lambda i: 10 + 0.5 * i,
    "falling-small": lambda i: 20 - 0.5 * i,
}
MONTHLY = [
    {
        "customer": name,
        "name": name.title(),
        "month": date(2026 + (i - 1) // 12, (i - 1) % 12 + 1, 1),
        "revenue": float(trend(i)),
    }
    for name, trend in TREND.items()
    for i in range(1, PERIODS + 1)
]


def matrix() -> BostonMatrix:
    """a fresh matrix: every derived collection is cached on the instance"""
    return BostonMatrix(MONTHLY, id_field="customer", sequence_field="month",
                        value_field="revenue", window_size=6)


def scatters(ax) -> list:
    """the scatter collections drawn on ax, in draw order"""
    return [c for c in ax.collections if isinstance(c, PathCollection)]


class TestParetoAnalysis(TestCase):

    def setUp(self):
        self.analysis = ParetoAnalysis(REVENUE, "revenue", "customer")

    def test_total(self):
        self.assertEqual(self.analysis.total, 1000.0)

    def test_sorted_descending(self):
        labels = [row["customer"] for row in self.analysis.calculated_data]
        self.assertEqual(labels, ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"])

    def test_added_fields(self):
        first = self.analysis.calculated_data[0]
        self.assertEqual(first["cumulative"], 500.0)
        self.assertEqual(first["percent"], 50.0)
        self.assertEqual(first["cum_percent"], 50.0)

    def test_cum_percent_ends_at_100(self):
        self.assertAlmostEqual(self.analysis.calculated_data[-1]["cum_percent"], 100.0)

    def test_input_rows_are_not_modified(self):
        """two analyses over the same rows must not see each other's fields"""
        self.analysis.calculated_data
        self.assertNotIn("cum_percent", REVENUE[0])

    def test_calculated_data_is_cached(self):
        self.assertIs(self.analysis.calculated_data, self.analysis.calculated_data)

    def test_top_n(self):
        self.assertEqual(
            [row["customer"] for row in self.analysis.top_n(2)], ["Alpha", "Beta"]
        )

    def test_top_n_percent_includes_the_crossing_row(self):
        """80% of this data is 75% plus Gamma: stopping at Beta reports 75%"""
        rows = self.analysis.top_n_percent(80.0)
        self.assertEqual([r["customer"] for r in rows], ["Alpha", "Beta", "Gamma"])
        self.assertGreaterEqual(rows[-1]["cum_percent"], 80.0)

    def test_top_n_percent_stops_on_an_exact_hit(self):
        rows = self.analysis.top_n_percent(75.0)
        self.assertEqual([r["customer"] for r in rows], ["Alpha", "Beta"])

    def test_top_n_percent_entities(self):
        entities = self.analysis.top_n_percent_entities(50.0)
        self.assertEqual(list(entities), ["Alpha"])
        self.assertEqual(entities["Alpha"]["revenue"], 500.0)

    def test_entities_needs_a_label_field(self):
        with self.assertRaises(DKitDataException):
            ParetoAnalysis(REVENUE, "revenue").top_n_percent_entities()

    def test_zero_total_is_refused(self):
        """every share would be undefined, not merely uninteresting"""
        with self.assertRaises(DKitDataException):
            ParetoAnalysis([{"v": 0}, {"v": 0}], "v").calculated_data

    def test_iterable_and_sized(self):
        self.assertEqual(len(self.analysis), 5)
        self.assertEqual(len(list(self.analysis)), 5)


class TestBostonMatrix(TestCase):

    def setUp(self):
        self.matrix = matrix()

    def test_aliases_carry_the_window_size(self):
        self.assertEqual(self.matrix.alias_median, "ma_6")
        self.assertEqual(self.matrix.alias_mean, "mean_6")
        self.assertEqual(self.matrix.alias_growth, "gr_6")

    def test_two_windows_do_not_collide(self):
        other = BostonMatrix(MONTHLY, "customer", "month", "revenue", window_size=3)
        self.assertNotEqual(other.alias_median, self.matrix.alias_median)

    def test_window_fields_added(self):
        row = self.matrix.windowed[0]
        for name in ("ma_6", "mean_6", "gr_6", "last_value"):
            self.assertIn(name, row)

    def test_last_sequence(self):
        self.assertEqual(self.matrix.last_sequence, date(2027, 6, 1))

    def test_last_interval_is_one_row_per_entity(self):
        rows = self.matrix.last_interval
        self.assertEqual(len(rows), 4)
        self.assertEqual(len({r["customer"] for r in rows}), 4)

    def test_median_cuts_the_population_in_half(self):
        median = self.matrix.median
        above = [r for r in self.matrix.last_interval if r["ma_6"] > median]
        self.assertEqual(len(above), 2)

    def test_quadrant_membership(self):
        expected = {
            1: "falling-big", 2: "rising-big", 3: "rising-small", 4: "falling-small",
        }
        for n, customer in expected.items():
            rows = self.matrix.quadrant(n)
            self.assertEqual([r["customer"] for r in rows], [customer])

    def test_quadrant_tags_rows(self):
        self.assertEqual(self.matrix.quadrant(2)[0]["quadrant"], "Q2")

    def test_classified_covers_every_entity_once(self):
        tags = [row["quadrant"] for row in self.matrix.classified]
        self.assertEqual(sorted(tags), sorted(QUADRANTS.values()))

    def test_classified_does_not_tag_the_windowed_rows(self):
        """the tag is presentation: last_interval stays the analysis output"""
        self.matrix.classified
        self.assertNotIn("quadrant", self.matrix.last_interval[0])

    def test_quadrant_of_is_the_only_classifier(self):
        for row in self.matrix.classified:
            self.assertEqual(QUADRANTS[self.matrix.quadrant_of(row)], row["quadrant"])

    def test_bad_quadrant(self):
        with self.assertRaises(DKitDataException):
            self.matrix.quadrant(5)

    def test_by_value_is_ranked(self):
        values = [row["revenue"] for row in self.matrix.by_value()]
        self.assertEqual(values, sorted(values, reverse=True))

    def test_growth_and_decline_partition_the_period(self):
        growth = self.matrix.by_growth()
        decline = self.matrix.by_decline()
        self.assertEqual(len(growth) + len(decline), len(self.matrix.last_interval))
        self.assertTrue(all(r["gr_6"] >= 0 for r in growth))
        self.assertTrue(all(r["gr_6"] < 0 for r in decline))

    def test_decline_is_worst_first(self):
        rates = [row["gr_6"] for row in self.matrix.by_decline()]
        self.assertEqual(rates, sorted(rates))

    def test_windowed_is_computed_once(self):
        self.assertIs(self.matrix.windowed, self.matrix.windowed)

    def test_empty_data(self):
        with self.assertRaises(DKitDataException):
            BostonMatrix([], "customer", "month", "revenue").last_sequence


class TestNoMatplotlibDependency(TestCase):
    """the point of the split: analysis without the plotting stack"""

    def _import_without_matplotlib(self, module: str):
        script = (
            "import sys\n"
            "class Blocker:\n"
            "    def find_module(self, name, path=None):\n"
            "        if name.split('.')[0] == 'matplotlib':\n"
            "            raise ImportError('matplotlib is blocked')\n"
            "sys.meta_path.insert(0, Blocker())\n"
            f"import {module}\n"
            "assert not [m for m in sys.modules if m.startswith('matplotlib')]\n"
            "print('ok')\n"
        )
        # the repo root, not "..": the subprocess must find dkit regardless of
        # the directory pytest was started from
        root = path.dirname(path.dirname(path.abspath(__file__)))
        return subprocess.run(
            [sys.executable, "-c", script], cwd=root, capture_output=True, text=True
        )

    def test_pareto_imports_without_matplotlib(self):
        result = self._import_without_matplotlib("dkit.data.pareto")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_boston_imports_without_matplotlib(self):
        result = self._import_without_matplotlib("dkit.data.boston")
        self.assertEqual(result.returncode, 0, result.stderr)


class TestControlChart(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_returns_a_figure(self):
        fig = canned.control_chart(CONTROL, x="month", y="sales", title="Sales")
        self.assertIsInstance(fig, Figure)

    def test_layers_drawn(self):
        fig = canned.control_chart(CONTROL, x="month", y="sales")
        ax = fig.axes[0]
        # the band, the centre line and the observations
        self.assertEqual(len(ax.collections), 2)      # fill_between and the scatter
        self.assertEqual(len(ax.lines), 2)

    def test_breaches_are_highlighted(self):
        """one row over the ucl and one under the lcl: two marked points"""
        fig = canned.control_chart(CONTROL, x="month", y="sales")
        marked = scatters(fig.axes[0])[0]
        self.assertEqual(len(marked.get_offsets()), 2)

    def test_no_breaches(self):
        rows = [dict(row, sales=100.0) for row in CONTROL]
        fig = canned.control_chart(rows, x="month", y="sales")
        self.assertEqual(len(scatters(fig.axes[0])[0].get_offsets()), 0)

    def test_draws_into_a_given_axes(self):
        fig, ax = plt.subplots()
        self.assertIs(canned.control_chart(CONTROL, x="month", y="sales", ax=ax), fig)

    def test_axis_labels(self):
        fig = canned.control_chart(CONTROL, x="month", y="sales", xlabel="Month",
                                   ylabel="Sales")
        ax = fig.axes[0]
        self.assertEqual(ax.get_xlabel(), "Month")
        self.assertEqual(ax.get_ylabel(), "Sales")

    def test_where_filters_before_the_layers(self):
        fig = canned.control_chart(CONTROL, x="month", y="sales",
                                   where="${sales} <= ${ucl} & ${sales} >= ${lcl}")
        self.assertEqual(len(scatters(fig.axes[0])[0].get_offsets()), 0)


class TestHistogram(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_returns_a_figure(self):
        self.assertIsInstance(canned.histogram(MONTHLY, "revenue"), Figure)

    def test_bars_and_a_mean_line(self):
        fig = canned.histogram(MONTHLY, "revenue", bins=5)
        ax = fig.axes[0]
        self.assertEqual(len(ax.containers), 1)
        self.assertEqual(len(ax.containers[0]), 5)
        self.assertEqual(len(ax.lines), 1)

    def test_mean_line_is_at_the_mean(self):
        fig = canned.histogram(MONTHLY, "revenue")
        expected = sum(r["revenue"] for r in MONTHLY) / len(MONTHLY)
        self.assertAlmostEqual(fig.axes[0].lines[0].get_xdata()[0], expected)

    def test_x_label_defaults_to_the_field(self):
        fig = canned.histogram(MONTHLY, "revenue")
        self.assertEqual(fig.axes[0].get_xlabel(), "revenue")

    def test_where_narrows_the_distribution(self):
        fig = canned.histogram(MONTHLY, "revenue", where="${revenue} < 50")
        self.assertLess(fig.axes[0].lines[0].get_xdata()[0], 50)


class TestPareto(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_returns_a_figure(self):
        fig = canned.pareto(REVENUE, value_field="revenue", label_field="customer")
        self.assertIsInstance(fig, Figure)

    def test_bars_ranked_and_a_curve_on_the_right(self):
        fig = canned.pareto(REVENUE, value_field="revenue", label_field="customer")
        left, right = fig.axes
        heights = [patch.get_height() for patch in left.containers[0]]
        self.assertEqual(heights, sorted(heights, reverse=True))
        self.assertAlmostEqual(right.lines[0].get_ydata()[-1], 100.0)

    def test_tick_labels_are_the_entities(self):
        fig = canned.pareto(REVENUE, value_field="revenue", label_field="customer")
        labels = [t.get_text() for t in fig.axes[0].get_xticklabels()]
        self.assertEqual(labels[0], "Alpha")

    def test_top_limits_the_bars_but_not_the_arithmetic(self):
        """the third bar must still read 90%, its share of the whole"""
        fig = canned.pareto(REVENUE, value_field="revenue", label_field="customer",
                            top=3)
        left, right = fig.axes
        self.assertEqual(len(left.containers[0]), 3)
        self.assertAlmostEqual(right.lines[0].get_ydata()[-1], 90.0)

    def test_an_analysis_is_reused(self):
        """the chart and the table must not compute the ranking twice"""
        analysis = ParetoAnalysis(REVENUE, "revenue", "customer")
        fig = canned.pareto(analysis)
        self.assertEqual(len(fig.axes[0].containers[0]), 5)

    def test_where_is_applied_before_the_analysis(self):
        fig = canned.pareto(REVENUE, value_field="revenue", label_field="customer",
                            where="${revenue} >= 150")
        self.assertEqual(len(fig.axes[0].containers[0]), 3)

    def test_draws_into_a_given_axes(self):
        fig, ax = plt.subplots()
        self.assertIs(
            canned.pareto(REVENUE, value_field="revenue", label_field="customer",
                          ax=ax),
            fig,
        )


class TestQuadrant(TestCase):

    def setUp(self):
        self.matrix = matrix()

    def tearDown(self):
        plt.close("all")

    def _figure(self, **kwargs):
        return canned.quadrant(
            self.matrix.classified, x=self.matrix.alias_growth,
            y=self.matrix.alias_median, y_center=self.matrix.median, **kwargs
        )

    def test_returns_a_figure(self):
        self.assertIsInstance(self._figure(), Figure)

    def test_one_point_per_entity(self):
        fig = self._figure()
        self.assertEqual(len(scatters(fig.axes[0])[0].get_offsets()), 4)

    def test_two_reference_lines_and_four_labels(self):
        ax = self._figure().axes[0]
        self.assertEqual(len(ax.lines), 2)
        self.assertEqual(len(ax.artists), 4)

    def test_cuts_are_where_the_analysis_says(self):
        ax = self._figure().axes[0]
        horizontal, vertical = ax.lines
        self.assertAlmostEqual(horizontal.get_ydata()[0], self.matrix.median)
        self.assertAlmostEqual(vertical.get_xdata()[0], 0.0)

    def test_y_center_defaults_to_the_median_of_the_rows_drawn(self):
        fig = canned.quadrant(self.matrix.classified, x=self.matrix.alias_growth,
                              y=self.matrix.alias_median)
        self.assertAlmostEqual(fig.axes[0].lines[0].get_ydata()[0], self.matrix.median)

    def test_a_filtered_chart_keeps_the_population_cut(self):
        """the point of passing the cuts in: half the rows, same divide"""
        fig = self._figure(where=f"${{{self.matrix.alias_growth}}} > 0")
        ax = fig.axes[0]
        self.assertEqual(len(scatters(ax)[0].get_offsets()), 2)
        self.assertAlmostEqual(ax.lines[0].get_ydata()[0], self.matrix.median)

    def test_quadrant_labels(self):
        ax = self._figure(quadrants=("A", "B", "C", "D")).axes[0]
        drawn = {artist.txt.get_text() for artist in ax.artists}
        self.assertEqual(drawn, {"A", "B", "C", "D"})


class TestParetoTable(TestCase):

    def setUp(self):
        self.analysis = ParetoAnalysis(REVENUE, "revenue", "customer")

    def test_columns(self):
        table = doc_canned.pareto_table(self.analysis)
        self.assertEqual(
            [column.name for column in table.columns],
            ["customer", "revenue", "cumulative", "percent", "cum_percent"],
        )

    def test_rows_are_the_analysis_rows(self):
        table = doc_canned.pareto_table(self.analysis)
        self.assertEqual(table.data[0]["customer"], "Alpha")

    def test_n_limits_the_rows(self):
        self.assertEqual(len(doc_canned.pareto_table(self.analysis, n=2).data), 2)

    def test_cum_percent_limits_the_rows(self):
        table = doc_canned.pareto_table(self.analysis, cum_percent=80.0)
        self.assertEqual(len(table.data), 3)

    def test_values_are_formatted(self):
        table = doc_canned.pareto_table(self.analysis)
        value = next(c for c in table.columns if c.name == "revenue")
        self.assertEqual(value.formatter(table.data[0]), "R500")

    def test_percent_is_formatted_as_a_percentage(self):
        table = doc_canned.pareto_table(self.analysis)
        column = next(c for c in table.columns if c.name == "cum_percent")
        self.assertTrue(column.formatter(table.data[0]).endswith("%"))

    def test_headings_default_to_the_field_name(self):
        table = doc_canned.pareto_table(self.analysis)
        self.assertEqual(table.columns[0].title, "customer")
        self.assertEqual(
            doc_canned.pareto_table(self.analysis, label_title="Customer").columns[0].title,
            "Customer",
        )


class TestBostonTables(TestCase):

    def setUp(self):
        self.matrix = matrix()
        self.tables = doc_canned.BostonTables(self.matrix, description_field="name")

    def test_standard_columns(self):
        table = self.tables.top_by_value()
        self.assertEqual(
            [column.name for column in table.columns],
            ["customer", "name", "revenue", "ma_6", "gr_6"],
        )

    def test_description_column_is_dropped_when_there_is_none(self):
        tables = doc_canned.BostonTables(self.matrix)
        names = [column.name for column in tables.columns()]
        self.assertEqual(names, ["customer", "revenue", "ma_6", "gr_6"])

    def test_window_size_is_substituted_into_titles(self):
        titles = [column.title for column in self.tables.columns()]
        self.assertIn("Median(6)", titles)
        self.assertIn("Growth(6)", titles)

    def test_last_value_heading_defaults_to_the_period(self):
        column = self.tables.col_last_value()
        self.assertEqual(column.title, str(self.matrix.last_sequence))

    def test_top_n_limits_every_table(self):
        tables = doc_canned.BostonTables(self.matrix, top_n=2)
        self.assertEqual(len(tables.top_by_value().data), 2)

    def test_ranked_tables(self):
        self.assertEqual(self.tables.top_by_value().data[0]["customer"], "rising-big")
        self.assertEqual(self.tables.top_by_decline().data[0]["gr_6"] < 0, True)

    def test_quadrant_table(self):
        table = self.tables.quadrant(2)
        self.assertEqual([row["customer"] for row in table.data], ["rising-big"])

    def test_values_are_formatted_as_currency(self):
        table = self.tables.top_by_value()
        column = next(c for c in table.columns if c.name == "revenue")
        self.assertTrue(column.formatter(table.data[0]).startswith("R"))

    def test_custom_columns_start_from_the_standard_ones(self):
        columns = self.tables.columns() + [self.tables.col_mean()]
        table = doc.Table(self.matrix.by_value(), columns)
        self.assertEqual(table.columns[-1].name, "mean_6")


class TestInAReport(TestCase):
    """acceptance: a canned figure and table in a built document"""

    def tearDown(self):
        plt.close("all")

    def test_figure_and_table_render(self):
        analysis = ParetoAnalysis(REVENUE, "revenue", "customer")
        with tempfile.TemporaryDirectory() as folder:
            image = path.join(folder, "pareto.png")

            @doc.wrap_matplotlib(filename=image)
            def chart():
                return canned.pareto(analysis, title="Revenue concentration")

            report = doc.Document(title="Canned report", author="test")
            report.add_template("## Revenue\n\n{{ chart() }}\n", chart=chart)
            report.add_element(doc_canned.pareto_table(analysis, n=3))

            self.assertTrue(path.exists(image))
            kinds = [type(element) for element in report.elements]
            self.assertIn(doc.Image, kinds)
            self.assertIn(doc.Table, kinds)

            output = path.join(folder, "report.html")
            HtmlRenderer(report).render(output)
            with open(output) as infile:
                html = infile.read()
        self.assertIn("pareto.png", html)
        self.assertIn("Alpha", html)
        # a figure wrapped without a title has no caption: Image.title has to
        # admit None, or from_dict turns the null into the string "None"
        self.assertNotIn(">None<", html)


if __name__ == "__main__":
    main()
