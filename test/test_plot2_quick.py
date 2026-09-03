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
tests for the dkit.plot2 tier B quick API, scale inference and faceting
"""
import sys; sys.path.insert(0, "..")  # noqa
import random
from datetime import date, datetime
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                          # noqa: E402
from matplotlib.collections import PolyCollection         # noqa: E402
from matplotlib.dates import AutoDateLocator              # noqa: E402
from matplotlib.ticker import AutoLocator, FixedLocator   # noqa: E402

from dkit.exceptions import DKitPlotException             # noqa: E402
from dkit.plot2 import Plot, geom, quick, scale           # noqa: E402


DATA = [
    {"month": "Jan", "day": date(2026, 1, 31), "sales": 100.0, "cost": 80.0,
     "region": "north"},
    {"month": "Feb", "day": date(2026, 2, 28), "sales": 150.0, "cost": 85.0,
     "region": "south"},
    {"month": "Mar", "day": date(2026, 3, 31), "sales": 90.0, "cost": 70.0,
     "region": "north"},
    {"month": "Apr", "day": date(2026, 4, 30), "sales": 120.0, "cost": 95.0,
     "region": "east"},
]


class TestScaleInfer(TestCase):

    def test_string_gives_categorical(self):
        self.assertIsInstance(scale.infer(["a", "b"]), scale.Categorical)

    def test_date_gives_time(self):
        self.assertIsInstance(scale.infer([date(2026, 1, 1)]), scale.Time)

    def test_datetime_gives_time(self):
        self.assertIsInstance(scale.infer([datetime(2026, 1, 1)]), scale.Time)

    def test_number_gives_linear(self):
        self.assertIsInstance(scale.infer([1, 2.5]), scale.Linear)

    def test_bool_gives_categorical(self):
        """True and False read as two categories, not as 1 and 0"""
        self.assertIsInstance(scale.infer([True, False]), scale.Categorical)

    def test_empty_gives_linear(self):
        self.assertIsInstance(scale.infer([]), scale.Linear)

    def test_leading_none_is_skipped(self):
        self.assertIsInstance(scale.infer([None, None, "a"]), scale.Categorical)

    def test_all_none_gives_linear(self):
        self.assertIsInstance(scale.infer([None, None]), scale.Linear)

    def test_label_and_kwargs_reach_the_scale(self):
        s = scale.infer(["a"], "Month", rotation=45)
        self.assertEqual(s.label, "Month")
        self.assertEqual(s.rotation, 45)


class TestQuickInference(TestCase):
    """the axis type is chosen from the data, which is what makes quick quick"""

    def locators(self, fig):
        ax = fig.axes[0]
        return (type(ax.xaxis.get_major_locator()), type(ax.yaxis.get_major_locator()))

    def test_categorical_x(self):
        fig = quick.bar(DATA, x="month", y="sales")
        self.assertEqual(self.locators(fig), (FixedLocator, AutoLocator))
        self.assertEqual([t.get_text() for t in fig.axes[0].get_xticklabels()],
                         ["Jan", "Feb", "Mar", "Apr"])
        plt.close(fig)

    def test_time_x(self):
        fig = quick.line(DATA, x="day", y="sales")
        self.assertEqual(self.locators(fig), (AutoDateLocator, AutoLocator))
        plt.close(fig)

    def test_numeric_x(self):
        fig = quick.scatter(DATA, x="cost", y="sales")
        self.assertEqual(self.locators(fig), (AutoLocator, AutoLocator))
        plt.close(fig)

    def test_categorical_y_for_horizontal_bars(self):
        fig = quick.bar(DATA, x="sales", y="month", horizontal=True)
        self.assertEqual(self.locators(fig), (AutoLocator, FixedLocator))
        plt.close(fig)

    def test_omitted_x_uses_the_row_index(self):
        fig = quick.line(DATA, y="sales")
        self.assertEqual(list(fig.axes[0].lines[0].get_xdata()), [0, 1, 2, 3])
        plt.close(fig)

    def test_explicit_scale_overrides_inference(self):
        fig = quick.scatter(DATA, x="cost", y="sales", xscale=scale.Log())
        self.assertEqual(fig.axes[0].get_xscale(), "log")
        plt.close(fig)

    def test_explicit_scale_keeps_its_own_label(self):
        """xlabel must not silently override the scale the caller passed"""
        fig = quick.line(DATA, x="day", y="sales", xscale=scale.Time("Date"))
        self.assertEqual(fig.axes[0].get_xlabel(), "Date")
        plt.close(fig)


class TestQuickFunctions(TestCase):

    def test_bar(self):
        fig = quick.bar(DATA, x="month", y="sales")
        self.assertEqual(len(fig.axes[0].containers[0].patches), 4)
        plt.close(fig)

    def test_bar_signed(self):
        fig = quick.bar(DATA, x="month", y="sales", color="signed")
        self.assertEqual(len(fig.axes[0].containers[0].patches), 4)
        plt.close(fig)

    def test_line(self):
        fig = quick.line(DATA, x="month", y="sales", marker="o", style="--")
        line = fig.axes[0].lines[0]
        self.assertEqual(line.get_marker(), "o")
        self.assertEqual(line.get_linestyle(), "--")
        plt.close(fig)

    def test_area(self):
        fig = quick.area(DATA, x="month", y="sales")
        fills = [c for c in fig.axes[0].collections if isinstance(c, PolyCollection)]
        self.assertEqual(len(fills), 1)
        plt.close(fig)

    def test_stem(self):
        fig = quick.stem(DATA, x="month", y="sales")
        self.assertEqual(len(fig.axes[0].containers[0].markerline.get_xdata()), 4)
        plt.close(fig)

    def test_scatter(self):
        fig = quick.scatter(DATA, x="cost", y="sales", size="sales")
        self.assertEqual(list(fig.axes[0].collections[0].get_sizes()),
                         [100.0, 150.0, 90.0, 120.0])
        plt.close(fig)

    def test_titles_and_labels(self):
        fig = quick.bar(DATA, x="month", y="sales", title="Sales",
                        xlabel="Month", ylabel="Rand")
        ax = fig.axes[0]
        self.assertEqual(ax.get_xlabel(), "Month")
        self.assertEqual(ax.get_ylabel(), "Rand")
        titles = [ax.get_title(loc=loc) for loc in ("left", "center", "right")]
        self.assertIn("Sales", titles)
        plt.close(fig)

    def test_label_draws_a_legend(self):
        fig = quick.line(DATA, x="month", y="sales", label="Sales", legend=True)
        self.assertEqual(
            [t.get_text() for t in fig.axes[0].get_legend().get_texts()], ["Sales"]
        )
        plt.close(fig)

    def test_where_filters(self):
        fig = quick.line(DATA, x="month", y="sales", where="${sales} > 95")
        self.assertEqual(len(fig.axes[0].lines[0].get_ydata()), 3)
        plt.close(fig)

    def test_theme_by_name(self):
        fig = quick.bar(DATA, x="month", y="sales", theme="dkit-dark")
        self.assertEqual(len(fig.axes[0].containers[0].patches), 4)
        plt.close(fig)

    def test_options_reach_the_plot(self):
        """**options is the escape hatch onto Plot's own arguments"""
        fig = quick.bar(DATA, x="month", y="sales", width=0.4, legend=False)
        self.assertAlmostEqual(fig.axes[0].containers[0].patches[0].get_width(), 0.4)
        plt.close(fig)

    def test_unknown_option_is_rejected(self):
        with self.assertRaises(TypeError):
            quick.bar(DATA, x="month", y="sales", nonsense=1)


class TestQuickAxEscapeHatch(TestCase):
    """every entry point accepts ax=, which is what stops plot2 dead-ending"""

    def test_returns_the_supplied_figure(self):
        fig, ax = plt.subplots()
        result = quick.bar(DATA, x="month", y="sales", ax=ax)
        self.assertIs(result, fig)
        plt.close(fig)

    def test_creates_no_extra_figure(self):
        fig, ax = plt.subplots()
        before = len(plt.get_fignums())
        quick.line(DATA, x="month", y="sales", ax=ax)
        self.assertEqual(len(plt.get_fignums()), before)
        plt.close(fig)

    def test_several_quick_calls_share_one_grid(self):
        fig, axes = plt.subplots(1, 2)
        quick.bar(DATA, x="month", y="sales", ax=axes[0])
        quick.line(DATA, x="month", y="cost", ax=axes[1])
        self.assertEqual(len(axes[0].containers), 1)
        self.assertEqual(len(axes[1].lines), 1)
        plt.close(fig)


class TestHist(TestCase):

    def setUp(self):
        random.seed(42)
        self.values = [{"x": random.gauss(10, 2)} for _ in range(400)]

    def test_bins_and_draws(self):
        fig = quick.hist(self.values, "x", bins=10)
        patches = fig.axes[0].containers[0].patches
        self.assertEqual(len(patches), 10)
        self.assertEqual(sum(p.get_height() for p in patches), 400)
        plt.close(fig)

    def test_auto_bins(self):
        """None auto-selects by the Freedman-Diaconis rule"""
        fig = quick.hist(self.values, "x")
        self.assertGreater(len(fig.axes[0].containers[0].patches), 1)
        plt.close(fig)

    def test_bars_use_each_bin_width(self):
        """equal-width bars would misreport an uneven final bin"""
        fig = quick.hist(self.values, "x", bins=10)
        patches = fig.axes[0].containers[0].patches
        widths = [p.get_width() for p in patches]
        self.assertTrue(all(w > 0 for w in widths))
        # bars are contiguous: each starts where the previous one ended
        for left, right in zip(patches, patches[1:]):
            self.assertAlmostEqual(left.get_x() + left.get_width(), right.get_x(),
                                   places=5)
        plt.close(fig)

    def test_x_axis_is_linear_not_categorical(self):
        fig = quick.hist(self.values, "x", bins=10)
        self.assertIsInstance(fig.axes[0].xaxis.get_major_locator(), AutoLocator)
        plt.close(fig)

    def test_labels_default_to_the_field_and_frequency(self):
        fig = quick.hist(self.values, "x", bins=5)
        self.assertEqual(fig.axes[0].get_xlabel(), "x")
        self.assertEqual(fig.axes[0].get_ylabel(), "Frequency")
        plt.close(fig)

    def test_where_filters_before_binning(self):
        fig = quick.hist(self.values, "x", bins=5, where="${x} > 10")
        total = sum(p.get_height() for p in fig.axes[0].containers[0].patches)
        self.assertLess(total, 400)
        plt.close(fig)


class TestBarWidthField(TestCase):
    """Bar(width="field") is what quick.hist needs; it is generally useful"""

    def test_width_per_row(self):
        rows = [{"x": 0.0, "y": 1.0, "w": 0.5}, {"x": 2.0, "y": 2.0, "w": 1.5}]
        fig = Plot(geom.Bar("a", x="x", y="y", width="w")).render(rows)
        widths = [p.get_width() for p in fig.axes[0].containers[0].patches]
        self.assertEqual(widths, [0.5, 1.5])
        plt.close(fig)

    def test_constant_width_still_works(self):
        rows = [{"x": 0.0, "y": 1.0}, {"x": 2.0, "y": 2.0}]
        fig = Plot(geom.Bar("a", x="x", y="y", width=0.25)).render(rows)
        widths = [p.get_width() for p in fig.axes[0].containers[0].patches]
        self.assertEqual(widths, [0.25, 0.25])
        plt.close(fig)


class TestFacet(TestCase):

    def spec(self, **kwargs):
        return Plot(
            geom.Bar("Sales", x="month", y="sales"),
            x=scale.Categorical("Month"),
            y=scale.Linear("Rand"),
            **kwargs,
        )

    def test_one_panel_per_group(self):
        fig = self.spec().facet(DATA, by="region")
        self.assertEqual(len(fig.axes), 3)
        plt.close(fig)

    def test_unused_panels_are_removed(self):
        """a 3 group, 2 column grid has 4 slots and must not show an empty one"""
        fig = self.spec().facet(DATA, by="region", ncols=2)
        self.assertEqual(len(fig.axes), 3)
        plt.close(fig)

    def test_panel_order_follows_the_rows(self):
        fig = self.spec().facet(DATA, by="region", ncols=3)
        titles = [ax.get_title(loc="left") for ax in fig.axes]
        self.assertEqual(titles, ["north", "south", "east"])
        plt.close(fig)

    def test_titles_can_be_suppressed(self):
        fig = self.spec().facet(DATA, by="region", titles=False)
        self.assertEqual([ax.get_title(loc="left") for ax in fig.axes], ["", "", ""])
        plt.close(fig)

    def test_plot_title_becomes_the_figure_title(self):
        fig = self.spec(title="Sales by region").facet(DATA, by="region")
        self.assertEqual(fig._suptitle.get_text(), "Sales by region")
        plt.close(fig)

    def test_discrete_domain_is_shared_across_panels(self):
        """the whole point: category 3 sits in the same place in every panel"""
        fig = self.spec().facet(DATA, by="region", ncols=3)
        for ax in fig.axes:
            self.assertEqual([t.get_text() for t in ax.get_xticklabels()],
                             ["Jan", "Feb", "Mar", "Apr"])
        plt.close(fig)

    def test_a_group_missing_a_category_keeps_the_full_domain(self):
        """east has only April: its bar must still be in the April slot"""
        fig = self.spec().facet(DATA, by="region", ncols=3)
        east = fig.axes[2]
        self.assertEqual(len(east.containers[0].patches), 1)
        self.assertAlmostEqual(east.containers[0].patches[0].get_x() + 0.4, 3.0)
        plt.close(fig)

    def test_continuous_limits_are_shared(self):
        fig = self.spec().facet(DATA, by="region", ncols=3)
        limits = {ax.get_ylim() for ax in fig.axes}
        self.assertEqual(len(limits), 1)
        plt.close(fig)

    def test_sharing_can_be_switched_off(self):
        fig = self.spec().facet(DATA, by="region", ncols=3, share_y=False)
        limits = {ax.get_ylim() for ax in fig.axes}
        self.assertGreater(len(limits), 1)
        plt.close(fig)

    def test_axis_labels_only_on_the_outer_panels(self):
        fig = self.spec().facet(DATA, by="region", ncols=2)
        # panels at (0,0) (0,1) (1,0); the last ncols are bottom-most
        self.assertEqual([ax.get_ylabel() for ax in fig.axes], ["Rand", "", "Rand"])
        self.assertEqual([ax.get_xlabel() for ax in fig.axes], ["", "Month", "Month"])
        plt.close(fig)

    def test_all_labels_when_nothing_is_shared(self):
        fig = self.spec().facet(DATA, by="region", ncols=2,
                                share_x=False, share_y=False)
        self.assertEqual([ax.get_xlabel() for ax in fig.axes],
                         ["Month"] * 3)
        plt.close(fig)

    def test_bottom_panel_above_a_removed_one_keeps_its_tick_labels(self):
        """sharex hides inner tick labels; the panel above a gap is not inner"""
        fig = self.spec().facet(DATA, by="region", ncols=2)
        stranded = fig.axes[1]                     # (0,1), nothing below it
        self.assertTrue(any(t.get_text() for t in stranded.get_xticklabels()))
        plt.close(fig)

    def test_one_legend_for_the_figure(self):
        fig = self.spec(legend=True).facet(DATA, by="region", ncols=3)
        self.assertIsNotNone(fig.axes[0].get_legend())
        self.assertIsNone(fig.axes[1].get_legend())
        plt.close(fig)

    def test_where_applies_before_grouping(self):
        # the expression grammar takes double-quoted strings only
        fig = self.spec(where='${region} != "east"').facet(DATA, by="region")
        self.assertEqual(len(fig.axes), 2)
        plt.close(fig)

    def test_panel_size_is_configurable(self):
        small = self.spec().facet(DATA, by="region", ncols=3, panel_width=4.0)
        large = self.spec().facet(DATA, by="region", ncols=3, panel_width=8.0)
        self.assertAlmostEqual(large.get_figwidth(), small.get_figwidth() * 2)
        plt.close("all")

    def test_grid_grows_with_the_group_count(self):
        one_row = self.spec().facet(DATA, by="region", ncols=3)
        two_rows = self.spec().facet(DATA, by="region", ncols=2)
        self.assertGreater(two_rows.get_figheight(), one_row.get_figheight())
        plt.close("all")

    def test_spec_is_reusable_after_faceting(self):
        p = self.spec()
        p.facet(DATA, by="region")
        fig = p.render(DATA)
        self.assertEqual(len(fig.axes), 1)
        self.assertEqual(len(fig.axes[0].containers[0].patches), 4)
        plt.close("all")

    def test_rcparams_are_not_leaked(self):
        before = dict(matplotlib.rcParams)
        self.spec(theme="dkit-dark").facet(DATA, by="region")
        self.assertEqual(matplotlib.rcParams["axes.facecolor"],
                         before["axes.facecolor"])
        plt.close("all")

    def test_bad_ncols(self):
        with self.assertRaises(DKitPlotException):
            self.spec().facet(DATA, by="region", ncols=0)

    def test_empty_data(self):
        with self.assertRaises(DKitPlotException):
            self.spec().facet([], by="region")

    def test_unknown_group_field(self):
        with self.assertRaises(DKitPlotException):
            self.spec().facet(DATA, by="nonexistent")


if __name__ == "__main__":
    main()
