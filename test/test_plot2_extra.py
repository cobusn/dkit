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
tests for the dkit.plot2 specialised plots: TreeMap, SlopePlot, HeatMap and
CalendarHeatmap

Covers the three surfaces each one has: the standalone class in
matplotlib_extra, the geom adapter inside a Plot, and the quick function.
"""
import sys; sys.path.insert(0, "..")  # noqa
from datetime import date                                     # noqa: E402
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                              # noqa: E402
import numpy as np                                           # noqa: E402
from matplotlib.collections import QuadMesh                   # noqa: E402
from matplotlib.colors import LogNorm, to_hex                 # noqa: E402
from matplotlib.patches import FancyBboxPatch, PathPatch, Rectangle  # noqa: E402
from matplotlib.ticker import FixedLocator                    # noqa: E402

from dkit.exceptions import DKitPlotException                 # noqa: E402
from dkit.plot2 import Plot, geom, get_theme, quick, scale     # noqa: E402
from dkit.plot2.matplotlib_extra import (                     # noqa: E402
    CalendarHeatmap, SlopePlot, TreeMap, contrast_color,
)


#: countries with a region, sized two different ways.  The same rows drive the
#: treemap and the slope plot, so the two are easy to compare while reading.
COUNTRIES = [
    {"country": "Alpha", "region": "north", "gdp": 100.0, "debt": 30.0},
    {"country": "Beta", "region": "north", "gdp": 60.0, "debt": 55.0},
    {"country": "Gamma", "region": "south", "gdp": 30.0, "debt": 12.0},
    {"country": "Delta", "region": "east", "gdp": 18.0, "debt": 9.0},
]

#: three months by two regions, with one pair deliberately missing
GRID = [
    {"month": "Jan", "region": "north", "sales": 10.0},
    {"month": "Jan", "region": "south", "sales": 20.0},
    {"month": "Feb", "region": "north", "sales": 30.0},
    {"month": "Mar", "region": "north", "sales": 40.0},
    {"month": "Mar", "region": "south", "sales": 50.0},
]

#: two years, three series, one of which is absent in the first year
MOVES = [
    {"name": "Alpha", "year": 2020, "value": 10.0},
    {"name": "Beta", "year": 2020, "value": 20.0},
    {"name": "Alpha", "year": 2024, "value": 25.0},
    {"name": "Beta", "year": 2024, "value": 15.0},
    {"name": "Gamma", "year": 2024, "value": 30.0},
]

#: ten days spanning a year boundary, one value per day
DAYS = [
    {"date": date(2023, 12, 29), "value": -2.0, "kind": "weekday"},
    {"date": date(2023, 12, 30), "value": -1.0, "kind": "weekend"},
    {"date": date(2023, 12, 31), "value": 0.0, "kind": "weekend"},
    {"date": date(2024, 1, 1), "value": 1.0, "kind": "weekday"},
    {"date": date(2024, 1, 2), "value": 2.0, "kind": "weekday"},
    {"date": date(2024, 1, 3), "value": 3.0, "kind": "weekday"},
    {"date": date(2024, 1, 4), "value": 4.0, "kind": "weekday"},
    {"date": date(2024, 1, 5), "value": 5.0, "kind": "weekday"},
    {"date": date(2024, 1, 6), "value": 6.0, "kind": "weekend"},
    {"date": date(2024, 1, 7), "value": 7.0, "kind": "weekend"},
]


def cells(ax) -> list:
    """the treemap rectangles on ``ax``"""
    return [p for p in ax.patches if isinstance(p, Rectangle)]


def texts(ax) -> list:
    """the text drawn on ``ax``, excluding the title"""
    return [t.get_text() for t in ax.texts]


class TestContrastColor(TestCase):
    """cell text has to be readable against a data driven fill"""

    def test_dark_fill_gets_light_text(self):
        self.assertEqual(contrast_color("#05386E"), "white")

    def test_light_fill_gets_dark_text(self):
        self.assertEqual(contrast_color("#f2f2f2"), "black")

    def test_accepts_an_rgba_tuple(self):
        self.assertEqual(contrast_color((1.0, 1.0, 1.0, 1.0)), "black")


class TestTreeMap(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_one_cell_per_row(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp")
        self.assertEqual(len(cells(ax)), 4)

    def test_area_is_proportional_to_value(self):
        """the whole point: twice the value, twice the area"""
        rows = [{"k": "a", "v": 2.0}, {"k": "b", "v": 1.0}]
        fig, ax = TreeMap().draw(rows, "k", "v")
        areas = sorted(c.get_width() * c.get_height() for c in cells(ax))
        self.assertAlmostEqual(areas[1] / areas[0], 2.0, places=4)

    def test_cells_are_drawn_largest_first(self):
        """squarify assumes descending input, whatever order the rows are in"""
        shuffled = [COUNTRIES[2], COUNTRIES[0], COUNTRIES[3], COUNTRIES[1]]
        fig, ax = TreeMap().draw(shuffled, "country", "gdp")
        areas = [c.get_width() * c.get_height() for c in cells(ax)]
        self.assertEqual(areas, sorted(areas, reverse=True))

    def test_labels_and_values(self):
        fig, ax = TreeMap(value_format="{:,.0f}").draw(COUNTRIES, "country", "gdp")
        self.assertIn("Alpha\n100", texts(ax))

    def test_no_value_format_labels_the_name_only(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp")
        self.assertIn("Alpha", texts(ax))

    def test_labels_are_top_left_aligned_and_smaller_than_body_text(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp")
        labels = [artist for artist in ax.texts if artist.get_text()]

        self.assertTrue(labels)
        self.assertTrue(all(label.get_ha() == "left" for label in labels))
        self.assertTrue(all(label.get_va() == "top" for label in labels))
        self.assertTrue(
            all(
                label.get_fontsize() < matplotlib.rcParams["font.size"]
                for label in labels
            )
        )

    def test_min_label_area_drops_small_cells(self):
        fig, ax = TreeMap(min_label_area=0.4).draw(COUNTRIES, "country", "gdp")
        # only Alpha is 40% of the total
        self.assertEqual(texts(ax), ["Alpha"])

    def test_labels_that_do_not_fit_are_removed(self):
        """min_label_area cannot see that a long name will not fit"""
        rows = [{"k": "a" * 200, "v": 1.0}, {"k": "b", "v": 1.0}]
        fig, ax = TreeMap(font_size=20).draw(rows, "k", "v")
        self.assertNotIn("a" * 200, texts(ax))

    def test_axes_furniture_is_off(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp")
        self.assertFalse(ax.axison)

    def test_title(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp", title="GDP")
        self.assertIn("GDP", [ax.get_title(loc=x) for x in ("left", "center", "right")])

    def test_where_filters(self):
        fig, ax = TreeMap().draw(COUNTRIES, "country", "gdp",
                                 where='${region} == "north"')
        self.assertEqual(len(cells(ax)), 2)

    def test_empty_data(self):
        with self.assertRaises(DKitPlotException):
            TreeMap().draw([], "country", "gdp")

    def test_negative_values_are_rejected(self):
        """area cannot be negative, and silently dropping the row would lie"""
        with self.assertRaises(DKitPlotException):
            TreeMap().draw([{"k": "a", "v": -1.0}], "k", "v")

    def test_bad_norm(self):
        with self.assertRaises(DKitPlotException):
            TreeMap(norm="quadratic")


class TestTreeMapColors(TestCase):
    """colour assignment is the one thing an instance remembers"""

    def tearDown(self):
        plt.close("all")

    def test_categories_take_the_theme_cycle(self):
        theme = get_theme("dkit-light")
        tm = TreeMap()
        tm.draw(COUNTRIES, "country", "gdp", color_field="region", theme=theme)
        self.assertEqual(list(tm.category_colors.values())[:3], list(theme.cycle())[:3])

    def test_one_color_per_category_not_per_row(self):
        tm = TreeMap()
        fig, ax = tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        self.assertEqual(len(tm.category_colors), 3)
        # Alpha and Beta are both north
        self.assertEqual(to_hex(cells(ax)[0].get_facecolor()),
                         to_hex(cells(ax)[1].get_facecolor()))

    def test_colors_carry_across_draws(self):
        """the reason this state exists: two plots readable side by side"""
        tm = TreeMap()
        tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        first = tm.category_colors
        # a different metric, and rows in a different order
        tm.draw(list(reversed(COUNTRIES)), "country", "debt", color_field="region")
        self.assertEqual(tm.category_colors, first)

    def test_a_new_category_takes_the_next_color(self):
        tm = TreeMap()
        tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        tm.draw(COUNTRIES + [{"country": "Eta", "region": "west", "gdp": 5.0}],
                "country", "gdp", color_field="region")
        self.assertEqual(len(tm.category_colors), 4)

    def test_reset_colors_starts_over(self):
        tm = TreeMap()
        tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        tm.reset_colors()
        self.assertEqual(tm.category_colors, {})
        tm.draw([r for r in COUNTRIES if r["region"] != "north"],
                "country", "gdp", color_field="region")
        # south was second before the reset and is first after it
        self.assertEqual(list(tm.category_colors), ["south", "east"])

    def test_explicit_color_list(self):
        tm = TreeMap(color_map=["#ff0000", "#00ff00", "#0000ff"])
        fig, ax = tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        self.assertEqual(to_hex(cells(ax)[0].get_facecolor()), "#ff0000")

    def test_categorical_use_of_a_continuous_map_spreads_out(self):
        """two categories off viridis must not be two shades of the same green"""
        tm = TreeMap(color_map="viridis")
        tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        assigned = [to_hex(c) for c in tm.category_colors.values()]
        self.assertEqual(len(set(assigned)), 3)

    def test_sequential_draws_a_colorbar(self):
        fig, ax = TreeMap(norm="linear").draw(COUNTRIES, "country", "gdp")
        self.assertEqual(len(fig.axes), 2)

    def test_sequential_colors_by_value(self):
        fig, ax = TreeMap(norm="linear").draw(COUNTRIES, "country", "gdp")
        # cells are drawn largest value first, and the colour map runs one way
        first, last = cells(ax)[0].get_facecolor(), cells(ax)[-1].get_facecolor()
        self.assertNotEqual(to_hex(first), to_hex(last))

    def test_log_norm(self):
        fig, ax = TreeMap(norm="log").draw(COUNTRIES, "country", "gdp")
        self.assertIsInstance(fig.axes[1]._colorbar.norm, LogNorm)

    def test_color_range_is_shared_not_remembered(self):
        tm = TreeMap(norm="linear")
        shared = (0.0, 200.0)
        fig1, _ = tm.draw(COUNTRIES, "country", "gdp", color_range=shared)
        fig2, _ = tm.draw(COUNTRIES, "country", "debt", color_range=shared)
        self.assertEqual(fig1.axes[1]._colorbar.vmin, fig2.axes[1]._colorbar.vmin)
        # omitting it auto-ranges from that call's own data
        fig3, _ = tm.draw(COUNTRIES, "country", "debt")
        self.assertNotEqual(fig3.axes[1]._colorbar.vmin, shared[0])

    def test_sequential_ignores_the_category_cache(self):
        tm = TreeMap(norm="linear")
        tm.draw(COUNTRIES, "country", "gdp", color_field="region")
        self.assertEqual(tm.category_colors, {})


class TestTreeMapFigure(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_figsize_is_centimetres(self):
        fig, ax = TreeMap(figsize=(10.0, 5.0)).draw(COUNTRIES, "country", "gdp")
        self.assertAlmostEqual(fig.get_figwidth(), 10.0 * 0.393701, places=3)

    def test_default_size_comes_from_the_theme(self):
        theme = get_theme("dkit-light").replace(width=12.0, height=8.0)
        fig, ax = TreeMap(theme=theme).draw(COUNTRIES, "country", "gdp")
        self.assertAlmostEqual(fig.get_figheight(), 8.0 * 0.393701, places=3)

    def test_draws_into_a_supplied_axes(self):
        fig, ax = plt.subplots()
        result_fig, result_ax = TreeMap().draw(COUNTRIES, "country", "gdp", ax=ax)
        self.assertIs(result_fig, fig)
        self.assertIs(result_ax, ax)

    def test_creates_no_extra_figure_when_given_an_axes(self):
        fig, ax = plt.subplots()
        before = len(plt.get_fignums())
        TreeMap().draw(COUNTRIES, "country", "gdp", ax=ax)
        self.assertEqual(len(plt.get_fignums()), before)

    def test_rcparams_are_not_leaked(self):
        before = matplotlib.rcParams["axes.facecolor"]
        TreeMap(theme="dkit-dark").draw(COUNTRIES, "country", "gdp")
        self.assertEqual(matplotlib.rcParams["axes.facecolor"], before)

    def test_cells_fill_a_wide_axes(self):
        """a square layout in a wide figure would pad down to a centred square"""
        fig, ax = TreeMap(figsize=(16.0, 6.0)).draw(COUNTRIES, "country", "gdp")
        covered = sum(c.get_width() * c.get_height() for c in cells(ax))
        x0, x1 = ax.get_xlim()
        y0, y1 = ax.get_ylim()
        self.assertAlmostEqual(covered / ((x1 - x0) * (y1 - y0)), 1.0, places=3)

    def test_cells_use_the_full_figure_without_a_title(self):
        """a standalone treemap does not retain subplot margins"""
        fig, ax = TreeMap(figsize=(16.0, 6.0)).draw(
            COUNTRIES, "country", "gdp"
        )
        position = ax.get_position()
        self.assertAlmostEqual(position.x0, 0.0)
        self.assertAlmostEqual(position.y0, 0.0)
        self.assertAlmostEqual(position.x1, 1.0)
        self.assertAlmostEqual(position.y1, 1.0)

    def test_titled_plot_reserves_only_a_small_top_band(self):
        """a treemap title gets a small dedicated top band"""
        fig, ax = TreeMap(figsize=(16.0, 6.0)).draw(
            COUNTRIES, "country", "gdp", title="GDP"
        )
        self.assertAlmostEqual(ax.get_position().y1, 0.90)

    def test_plot_wrapper_preserves_treemap_usable_area(self):
        """the Plot adapter applies the same compact exclusive layout"""
        fig = Plot(
            geom.TreeMap("country", "gdp"),
            title="GDP",
            width=16.0,
            height=6.0,
        ).render(COUNTRIES)
        position = fig.axes[0].get_position()
        self.assertAlmostEqual(position.x0, 0.0)
        self.assertAlmostEqual(position.y0, 0.0)
        self.assertAlmostEqual(position.x1, 1.0)
        self.assertAlmostEqual(position.y1, 0.90)


class TestSlopePlot(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_one_line_per_series(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value")
        self.assertEqual(len(ax.lines), 3)

    def test_columns_follow_first_appearance(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value")
        self.assertIn("2020", texts(ax))
        self.assertIn("2024", texts(ax))

    def test_explicit_column_order(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value", pivots=[2024, 2020])
        alpha = ax.lines[0]
        self.assertEqual(list(alpha.get_ydata()), [25.0, 10.0])

    def test_end_labels_carry_name_and_value(self):
        fig, ax = SlopePlot(value_format="{:,.0f}").draw(MOVES, "name", "year", "value")
        self.assertIn("Alpha, 10", texts(ax))
        self.assertIn("Alpha, 25", texts(ax))

    def test_a_series_missing_a_column_is_labelled_where_it_exists(self):
        """dkit.plot labelled the first and last column, so Gamma went unnamed"""
        fig, ax = SlopePlot(value_format="{:,.0f}").draw(MOVES, "name", "year", "value")
        self.assertIn("Gamma, 30", texts(ax))
        # and it is labelled once, not twice
        self.assertEqual(sum(1 for t in texts(ax) if t.startswith("Gamma")), 1)

    def test_a_missing_value_is_a_gap_not_a_zero(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value")
        gamma = ax.lines[2]
        self.assertIsNone(gamma.get_ydata()[0])
        self.assertEqual(gamma.get_ydata()[1], 30.0)

    def test_a_zero_value_is_still_labelled(self):
        """dkit.plot tested the value for truth, which dropped a real zero"""
        rows = [{"n": "a", "p": 1, "v": 0.0}, {"n": "a", "p": 2, "v": 5.0}]
        fig, ax = SlopePlot(value_format="{:,.0f}").draw(rows, "n", "p", "v")
        self.assertIn("a, 0", texts(ax))

    def test_label_width_truncates(self):
        rows = [{"n": "a very long series name", "p": 1, "v": 1.0}]
        fig, ax = SlopePlot(label_width=6).draw(rows, "n", "p", "v")
        self.assertIn("a very, 1.0", texts(ax))

    def test_label_width_none_keeps_the_whole_name(self):
        rows = [{"n": "a very long series name", "p": 1, "v": 1.0}]
        fig, ax = SlopePlot(label_width=None).draw(rows, "n", "p", "v")
        self.assertIn("a very long series name, 1.0", texts(ax))

    def test_a_single_column_series_is_labelled_once(self):
        rows = [{"n": "a", "p": 1, "v": 1.0}]
        fig, ax = SlopePlot().draw(rows, "n", "p", "v")
        self.assertEqual(sum(1 for t in texts(ax) if t.startswith("a,")), 1)

    def test_series_colors_carry_across_draws(self):
        sp = SlopePlot()
        sp.draw(MOVES, "name", "year", "value")
        first = sp.category_colors
        sp.draw([r for r in MOVES if r["name"] != "Alpha"], "name", "year", "value")
        self.assertEqual(sp.category_colors["Beta"], first["Beta"])

    def test_ticks_are_only_the_value_extremes(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value")
        self.assertEqual(list(ax.get_yticks()), [10.0, 30.0])
        self.assertEqual(list(ax.get_xticks()), [])

    def test_spines_are_hidden(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value")
        self.assertFalse(any(s.get_visible() for s in ax.spines.values()))

    def test_y_label_and_title(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value",
                                   y_label="Value", title="Moves")
        self.assertEqual(ax.get_ylabel(), "Value")
        self.assertIn("Moves", [ax.get_title(loc=x) for x in ("left", "center", "right")])

    def test_where_filters(self):
        fig, ax = SlopePlot().draw(MOVES, "name", "year", "value",
                                   where='${name} != "Gamma"')
        self.assertEqual(len(ax.lines), 2)

    def test_draws_into_a_supplied_axes(self):
        fig, ax = plt.subplots()
        result_fig, _ = SlopePlot().draw(MOVES, "name", "year", "value", ax=ax)
        self.assertIs(result_fig, fig)

    def test_a_constant_series_does_not_divide_by_zero(self):
        rows = [{"n": "a", "p": 1, "v": 5.0}, {"n": "a", "p": 2, "v": 5.0}]
        fig, ax = SlopePlot().draw(rows, "n", "p", "v")
        low, high = ax.get_ylim()
        self.assertLess(low, high)

    def test_empty_data(self):
        with self.assertRaises(DKitPlotException):
            SlopePlot().draw([], "name", "year", "value")

    def test_no_usable_values(self):
        rows = [{"n": "a", "p": 1, "v": None}]
        with self.assertRaises(DKitPlotException):
            SlopePlot().draw(rows, "n", "p", "v")


class TestHeatMap(TestCase):

    def mesh(self, fig) -> QuadMesh:
        return next(c for c in fig.axes[0].collections if isinstance(c, QuadMesh))

    def spec(self, **kwargs):
        return Plot(
            geom.HeatMap("Sales", x="month", y="region", z="sales", **kwargs),
            x=scale.Categorical("Month"),
            y=scale.Categorical("Region"),
        )

    def tearDown(self):
        plt.close("all")

    def test_grid_shape_follows_the_domains(self):
        fig = self.spec().render(GRID)
        self.assertEqual(self.mesh(fig).get_array().shape, (2, 3))

    def test_values_land_in_the_right_cells(self):
        fig = self.spec().render(GRID)
        grid = self.mesh(fig).get_array()
        # first appearance order: months Jan Feb Mar, regions north south
        self.assertEqual(grid[0, 0], 10.0)
        self.assertEqual(grid[1, 0], 20.0)
        self.assertEqual(grid[0, 2], 40.0)

    def test_a_missing_pair_is_masked_not_zero(self):
        """a gap and a real zero are different facts"""
        grid = self.mesh(self.spec().render(GRID)).get_array()
        self.assertIs(grid[1, 1], np.ma.masked)

    def test_a_repeated_pair_keeps_the_last_row(self):
        rows = GRID + [{"month": "Jan", "region": "north", "sales": 99.0}]
        grid = self.mesh(self.spec().render(rows)).get_array()
        self.assertEqual(grid[0, 0], 99.0)

    def test_axis_order_is_deterministic(self):
        """dkit.plot built its axes from a set, so they moved between runs"""
        labels = []
        for _ in range(3):
            fig = self.spec().render(GRID)
            labels.append([t.get_text() for t in fig.axes[0].get_xticklabels()])
            plt.close(fig)
        self.assertEqual(labels, [["Jan", "Feb", "Mar"]] * 3)

    def test_cells_are_centred_on_the_tick_positions(self):
        fig = self.spec().render(GRID)
        ax = fig.axes[0]
        self.assertIsInstance(ax.xaxis.get_major_locator(), FixedLocator)
        self.assertEqual(list(ax.get_xticks()), [0, 1, 2])
        self.assertEqual(ax.get_xlim(), (-0.5, 2.5))

    def test_colorbar_is_labelled_by_the_layer_label(self):
        fig = self.spec().render(GRID)
        self.assertEqual(len(fig.axes), 2)
        self.assertEqual(fig.axes[1].get_ylabel(), "Sales")

    def test_colorbar_can_be_switched_off(self):
        fig = self.spec(colorbar=False).render(GRID)
        self.assertEqual(len(fig.axes), 1)

    def test_no_legend_for_a_mesh(self):
        """a mesh handle in a legend says nothing, so label goes to the colour bar"""
        fig = Plot(
            geom.HeatMap("Sales", x="month", y="region", z="sales"),
            x=scale.Categorical(), y=scale.Categorical(), legend=True,
        ).render(GRID)
        self.assertIsNone(fig.axes[0].get_legend())

    def test_annotations(self):
        fig = self.spec(annotate=True, format="{:,.0f}").render(GRID)
        self.assertEqual(sorted(texts(fig.axes[0])),
                         ["10", "20", "30", "40", "50"])

    def test_no_annotation_in_an_empty_cell(self):
        fig = self.spec(annotate=True).render(GRID)
        self.assertEqual(len(texts(fig.axes[0])), 5)

    def test_shared_color_range(self):
        low = self.spec(vmin=0.0, vmax=100.0).render(GRID)
        self.assertEqual(self.mesh(low).norm.vmin, 0.0)
        self.assertEqual(self.mesh(low).norm.vmax, 100.0)

    def test_cmap_comes_from_the_theme(self):
        theme = get_theme("dkit-light")
        fig = self.spec().replace(theme=theme).render(GRID)
        self.assertEqual(self.mesh(fig).cmap.name, theme.sequential)

    def test_diverging_role(self):
        theme = get_theme("dkit-light")
        fig = self.spec(cmap="diverging").replace(theme=theme).render(GRID)
        self.assertEqual(self.mesh(fig).cmap.name, theme.diverging)

    def test_a_continuous_scale_is_rejected(self):
        """a cell covers one whole category, so the axis cannot be continuous"""
        with self.assertRaises(DKitPlotException):
            Plot(
                geom.HeatMap(x="month", y="region", z="sales"),
                x=scale.Linear(), y=scale.Categorical(),
            ).render(GRID)

    def test_facet_keeps_the_full_domain_in_every_panel(self):
        rows = [dict(r, quarter="Q1") for r in GRID] + [
            dict(r, quarter="Q2", sales=r["sales"] * 2) for r in GRID[:2]
        ]
        fig = self.spec(colorbar=False).replace().facet(rows, by="quarter", ncols=2)
        for ax in fig.axes:
            mesh = next(c for c in ax.collections if isinstance(c, QuadMesh))
            self.assertEqual(mesh.get_array().shape, (2, 3))
        plt.close(fig)


def day_cells(ax) -> list:
    """the day squares a CalendarHeatmap draws"""
    return [p for p in ax.patches if isinstance(p, FancyBboxPatch)]


class TestCalendarHeatmap(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_one_cell_per_day_in_range(self):
        fig, ax = CalendarHeatmap().draw(DAYS, "date", "value")
        self.assertEqual(len(day_cells(ax)), 10)

    def test_start_and_end_date_set_the_span(self):
        fig, ax = CalendarHeatmap().draw(
            DAYS, "date", "value",
            start_date=date(2024, 1, 1), end_date=date(2024, 1, 7),
        )
        self.assertEqual(len(day_cells(ax)), 7)

    def test_vcenter_centres_the_colour_scale(self):
        """a day worth 0, under vcenter=0, must sit at the colour map's midpoint"""
        cmap = get_theme("dkit-light").get_cmap("sequential")
        fig, ax = CalendarHeatmap(vcenter=0.0).draw(DAYS, "date", "value")
        zero_day = next(r for r, d in zip(day_cells(ax), DAYS) if d["value"] == 0.0)
        self.assertEqual(to_hex(zero_day.get_facecolor()), to_hex(cmap(0.5)))

    def test_color_map_colours_by_category(self):
        tm = CalendarHeatmap(color_map={"weekday": "#4477aa", "weekend": "#cc6677"})
        fig, ax = tm.draw(DAYS, "date", "kind")
        weekday_cell = next(r for r, d in zip(day_cells(ax), DAYS)
                            if d["kind"] == "weekday")
        self.assertEqual(to_hex(weekday_cell.get_facecolor()), "#4477aa")

    def test_axes_furniture_is_off(self):
        fig, ax = CalendarHeatmap().draw(DAYS, "date", "value")
        self.assertFalse(ax.axison)

    def test_title(self):
        fig, ax = CalendarHeatmap().draw(DAYS, "date", "value", title="Activity")
        self.assertIn("Activity", [ax.get_title(loc=x) for x in ("left", "center", "right")])

    def test_where_filters(self):
        """filtering out the earliest rows narrows the data-derived span itself"""
        fig, ax = CalendarHeatmap().draw(
            DAYS, "date", "value", where='${value} >= 1.0'
        )
        self.assertEqual(len(day_cells(ax)), 7)

    def test_empty_data(self):
        with self.assertRaises(DKitPlotException):
            CalendarHeatmap().draw([], "date", "value")

    def test_draws_into_a_supplied_axes(self):
        fig, ax = plt.subplots()
        result_fig, result_ax = CalendarHeatmap().draw(DAYS, "date", "value", ax=ax)
        self.assertIs(result_fig, fig)
        self.assertIs(result_ax, ax)

    def test_month_grid_off_by_default(self):
        fig, ax = CalendarHeatmap().draw(DAYS, "date", "value")
        self.assertFalse(any(isinstance(p, PathPatch) for p in ax.patches))

    def test_month_grid_draws_a_bounding_box(self):
        fig, ax = CalendarHeatmap(month_grid=True).draw(DAYS, "date", "value")
        self.assertTrue(any(isinstance(p, PathPatch) for p in ax.patches))

    def test_month_grid_kws_reach_the_box(self):
        fig, ax = CalendarHeatmap(
            month_grid=True, month_grid_kws={"edgecolor": "red"},
        ).draw(DAYS, "date", "value")
        box = next(p for p in ax.patches if isinstance(p, PathPatch))
        self.assertEqual(to_hex(box.get_edgecolor()), "#ff0000")


class TestStandaloneAdapters(TestCase):
    """the geom adapters are what give these plots themes, titles and facets"""

    def tearDown(self):
        plt.close("all")

    def test_treemap_layer_draws(self):
        fig = Plot(geom.TreeMap("country", "gdp", "region")).render(COUNTRIES)
        self.assertEqual(len(cells(fig.axes[0])), 4)

    def test_slope_layer_draws(self):
        fig = Plot(geom.Slope("name", "year", "value")).render(MOVES)
        self.assertEqual(len(fig.axes[0].lines), 3)

    def test_style_keywords_reach_the_wrapped_class(self):
        fig = Plot(geom.TreeMap("country", "gdp", value_format="{:,.0f}")).render(COUNTRIES)
        self.assertIn("Alpha\n100", texts(fig.axes[0]))

    def test_unknown_style_keyword_is_rejected(self):
        with self.assertRaises(TypeError):
            geom.TreeMap("country", "gdp", nonsense=1)

    def test_scales_are_not_applied(self):
        """a treemap under a set of axis ticks would be nonsense"""
        fig = Plot(geom.TreeMap("country", "gdp"),
                   x=scale.Categorical("Month")).render(COUNTRIES)
        self.assertEqual(fig.axes[0].get_xlabel(), "")

    def test_the_plot_title_is_still_applied(self):
        fig = Plot(geom.TreeMap("country", "gdp"), title="GDP").render(COUNTRIES)
        titles = [fig.axes[0].get_title(loc=x) for x in ("left", "center", "right")]
        self.assertIn("GDP", titles)

    def test_slope_takes_its_y_label_from_the_scale(self):
        fig = Plot(geom.Slope("name", "year", "value"),
                   y=scale.Linear("Value")).render(MOVES)
        self.assertEqual(fig.axes[0].get_ylabel(), "Value")

    def test_an_exclusive_layer_cannot_share_a_plot(self):
        with self.assertRaises(DKitPlotException):
            Plot(geom.TreeMap("country", "gdp"), geom.Line("x", y="gdp"))

    def test_exclusive_is_reported_by_the_plot(self):
        self.assertTrue(Plot(geom.TreeMap("country", "gdp")).exclusive)
        self.assertFalse(Plot(geom.Line("x", y="gdp")).exclusive)
        self.assertFalse(Plot().exclusive)

    def test_theme_and_size_reach_the_wrapped_class(self):
        fig = Plot(geom.TreeMap("country", "gdp"), theme="dkit-dark",
                   width=12.0, height=8.0).render(COUNTRIES)
        self.assertAlmostEqual(fig.get_figheight(), 8.0 * 0.393701, places=3)

    def test_layer_where_filters(self):
        fig = Plot(geom.TreeMap("country", "gdp",
                                where='${region} == "north"')).render(COUNTRIES)
        self.assertEqual(len(cells(fig.axes[0])), 2)

    def test_faceting_keeps_colors_stable_across_panels(self):
        """one wrapped instance is reused, which is why this works"""
        rows = [dict(r, year=y) for y in (2023, 2024) for r in COUNTRIES]
        layer = geom.TreeMap("country", "gdp", "region")
        fig = Plot(layer).facet(rows, by="year", ncols=2)
        self.assertEqual(len(fig.axes), 2)
        first = [to_hex(c.get_facecolor()) for c in cells(fig.axes[0])]
        second = [to_hex(c.get_facecolor()) for c in cells(fig.axes[1])]
        self.assertEqual(first, second)

    def test_a_shared_instance_shares_its_colors(self):
        tm = TreeMap()
        a = Plot(geom.TreeMap("country", "gdp", "region", plot=tm))
        b = Plot(geom.TreeMap("country", "debt", "region", plot=tm))
        a.render(COUNTRIES)
        assigned = dict(tm.category_colors)
        b.render(COUNTRIES)
        self.assertEqual(tm.category_colors, assigned)

    def test_the_spec_is_reusable(self):
        p = Plot(geom.TreeMap("country", "gdp", "region"))
        p.render(COUNTRIES)
        fig = p.render(COUNTRIES)
        self.assertEqual(len(cells(fig.axes[0])), 4)

    def test_calendar_heatmap_layer_draws(self):
        fig = Plot(geom.CalendarHeatmap("date", "value")).render(DAYS)
        self.assertEqual(len(day_cells(fig.axes[0])), 10)

    def test_calendar_heatmap_is_exclusive(self):
        self.assertTrue(Plot(geom.CalendarHeatmap("date", "value")).exclusive)

    def test_calendar_heatmap_start_end_date(self):
        fig = Plot(geom.CalendarHeatmap(
            "date", "value",
            start_date=date(2024, 1, 1), end_date=date(2024, 1, 7),
        )).render(DAYS)
        self.assertEqual(len(day_cells(fig.axes[0])), 7)

    def test_calendar_heatmap_facets_by_span(self):
        rows = [dict(r, year=r["date"].year) for r in DAYS]
        fig = Plot(geom.CalendarHeatmap("date", "value"), tight_layout=False) \
            .facet(rows, by="year", ncols=1)
        self.assertEqual(len(fig.axes), 2)
        plt.close(fig)

    def test_save(self):
        import tempfile
        import os
        p = Plot(geom.Slope("name", "year", "value"), title="Moves")
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "slope.png")
            p.save(MOVES, path)
            self.assertGreater(os.path.getsize(path), 0)


class TestQuickSpecialised(TestCase):

    def tearDown(self):
        plt.close("all")

    def test_treemap(self):
        fig = quick.treemap(COUNTRIES, "country", "gdp", color_field="region",
                            title="GDP", value_format="{:,.0f}")
        self.assertEqual(len(cells(fig.axes[0])), 4)
        self.assertIn("Alpha\n100", texts(fig.axes[0]))

    def test_treemap_sequential(self):
        fig = quick.treemap(COUNTRIES, "country", "gdp", norm="linear")
        self.assertEqual(len(fig.axes), 2)

    def test_treemap_color_range(self):
        fig = quick.treemap(COUNTRIES, "country", "gdp", norm="linear",
                            color_range=(0.0, 500.0))
        self.assertEqual(fig.axes[1]._colorbar.vmax, 500.0)

    def test_slope(self):
        fig = quick.slope(MOVES, "name", "year", "value", ylabel="Value",
                          value_format="{:,.0f}")
        self.assertEqual(len(fig.axes[0].lines), 3)
        self.assertEqual(fig.axes[0].get_ylabel(), "Value")

    def test_slope_pivot_order(self):
        fig = quick.slope(MOVES, "name", "year", "value", pivots=[2024, 2020])
        self.assertEqual(list(fig.axes[0].lines[0].get_ydata()), [25.0, 10.0])

    def test_heatmap(self):
        fig = quick.heatmap(GRID, "month", "region", "sales", label="Sales",
                            xlabel="Month", ylabel="Region")
        ax = fig.axes[0]
        self.assertEqual([t.get_text() for t in ax.get_xticklabels()],
                         ["Jan", "Feb", "Mar"])
        self.assertEqual(ax.get_xlabel(), "Month")
        self.assertEqual(fig.axes[1].get_ylabel(), "Sales")

    def test_heatmap_axes_are_categorical_not_inferred(self):
        """the months are strings anyway; the regions must not be guessed either"""
        fig = quick.heatmap(GRID, "month", "region", "sales")
        self.assertIsInstance(fig.axes[0].yaxis.get_major_locator(), FixedLocator)

    def test_heatmap_annotations(self):
        fig = quick.heatmap(GRID, "month", "region", "sales", annotate=True,
                            format="{:,.0f}")
        self.assertIn("50", texts(fig.axes[0]))

    def test_where_filters(self):
        fig = quick.treemap(COUNTRIES, "country", "gdp",
                            where='${region} == "north"')
        self.assertEqual(len(cells(fig.axes[0])), 2)

    def test_calendar_heatmap(self):
        fig = quick.calendar_heatmap(DAYS, "date", "value", title="Activity")
        self.assertEqual(len(day_cells(fig.axes[0])), 10)
        titles = [fig.axes[0].get_title(loc=x) for x in ("left", "center", "right")]
        self.assertIn("Activity", titles)

    def test_theme_by_name(self):
        fig = quick.slope(MOVES, "name", "year", "value", theme="dkit-dark")
        self.assertEqual(len(fig.axes[0].lines), 3)

    def test_ax_escape_hatch(self):
        fig, axes = plt.subplots(1, 3)
        quick.treemap(COUNTRIES, "country", "gdp", ax=axes[0])
        quick.slope(MOVES, "name", "year", "value", ax=axes[1])
        quick.heatmap(GRID, "month", "region", "sales", colorbar=False, ax=axes[2])
        self.assertEqual(len(cells(axes[0])), 4)
        self.assertEqual(len(axes[1].lines), 3)
        self.assertEqual(len([c for c in axes[2].collections
                              if isinstance(c, QuadMesh)]), 1)

    def test_options_reach_the_plot(self):
        fig = quick.treemap(COUNTRIES, "country", "gdp", width=12.0)
        self.assertAlmostEqual(fig.get_figwidth(), 12.0 * 0.393701, places=3)

    def test_unknown_option_is_rejected(self):
        with self.assertRaises(TypeError):
            quick.slope(MOVES, "name", "year", "value", nonsense=1)


if __name__ == "__main__":
    main()
