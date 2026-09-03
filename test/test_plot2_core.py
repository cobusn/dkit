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
tests for the dkit.plot2 rendering core: Frame, scale and Plot

Geoms arrive in a later milestone, so the layer used here is a minimal stub.
That is deliberate: it exercises the Layer contract without depending on any
geom implementation.
"""
import sys; sys.path.insert(0, "..")  # noqa
import os
from datetime import date
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib as mpl                      # noqa: E402
import matplotlib.pyplot as plt               # noqa: E402
from matplotlib.dates import AutoDateLocator   # noqa: E402
from matplotlib.ticker import FixedLocator, PercentFormatter, StrMethodFormatter  # noqa: E402

from dkit.exceptions import DKitPlotException  # noqa: E402
from dkit.plot2 import (                       # noqa: E402
    Frame, Layer, Plot, Theme, bundled_styles, save_figure, scale
)


DATA = [
    {"month": "Jan", "sales": 100.0, "cost": 60.0, "day": date(2026, 1, 31)},
    {"month": "Feb", "sales": 150.0, "cost": 90.0, "day": date(2026, 2, 28)},
    {"month": "Mar", "sales": 90.0, "cost": 120.0, "day": date(2026, 3, 31)},
]


class Line(Layer):
    """minimal Layer implementation, standing in for geom.Line"""

    def __init__(self, label=None, x=None, y=None, color=None, where=None, axis="left"):
        self.label = label
        self.x = x
        self.y = y
        self.color = color
        self.where = where
        self.axis = axis
        self.contexts = []

    def draw(self, ctx):
        self.contexts.append(ctx)
        ctx.ax.plot(
            ctx.x_values(self.x), ctx.y_values(self.y),
            color=ctx.color(self.color), label=self.label,
        )


class TestFrame(TestCase):

    def test_materialised_once(self):
        """a generator must survive being read by two layers"""
        frame = Frame(iter(DATA))
        self.assertEqual(frame.values("month"), ["Jan", "Feb", "Mar"])
        self.assertEqual(frame.values("month"), ["Jan", "Feb", "Mar"])

    def test_len_and_iter(self):
        frame = Frame(DATA)
        self.assertEqual(len(frame), 3)
        self.assertEqual([r["month"] for r in frame], ["Jan", "Feb", "Mar"])

    def test_implicit_index(self):
        """an omitted field falls back to the row index, with no magic key"""
        frame = Frame(DATA)
        self.assertEqual(frame.values(), [0, 1, 2])
        self.assertNotIn("_index", frame.rows[0])

    def test_where(self):
        frame = Frame(DATA, where="${sales} > 95")
        self.assertEqual(frame.values("month"), ["Jan", "Feb"])

    def test_filter_returns_self_when_empty(self):
        frame = Frame(DATA)
        self.assertIs(frame.filter(None), frame)

    def test_filter_does_not_mutate(self):
        frame = Frame(DATA)
        self.assertEqual(len(frame.filter("${sales} > 95")), 2)
        self.assertEqual(len(frame), 3)

    def test_fields(self):
        self.assertEqual(Frame(DATA).fields, ["month", "sales", "cost", "day"])

    def test_missing_field(self):
        with self.assertRaises(DKitPlotException) as ctx:
            Frame(DATA).values("nope")
        self.assertIn("month", str(ctx.exception))

    def test_distinct_preserves_order(self):
        rows = [{"k": "b"}, {"k": "a"}, {"k": "b"}]
        self.assertEqual(Frame(rows).distinct("k"), ["b", "a"])

    def test_groups(self):
        groups = Frame(DATA + [{"month": "Jan", "sales": 1.0}]).groups("month")
        self.assertEqual(list(groups), ["Jan", "Feb", "Mar"])
        self.assertEqual(len(groups["Jan"]), 2)

    def test_rejects_non_mappings(self):
        with self.assertRaises(DKitPlotException):
            Frame([1, 2, 3])

    def test_empty(self):
        frame = Frame([])
        self.assertEqual(len(frame), 0)
        self.assertEqual(frame.fields, [])

    def test_frame_of_frame(self):
        frame = Frame(Frame(DATA), where="${sales} > 95")
        self.assertEqual(len(frame), 2)


class TestScale(TestCase):

    def test_continuous_encode_is_identity(self):
        self.assertEqual(scale.Linear().encode([1.0, 2.5]), [1.0, 2.5])

    def test_categorical_encode(self):
        s = scale.Categorical()
        self.assertEqual(s.encode(["b", "a", "b"], ["b", "a"]), [0, 1, 0])

    def test_categorical_encode_shared_domain(self):
        """two layers sharing a category land on the same position"""
        s = scale.Categorical()
        domain = ["Jan", "Feb", "Mar"]
        self.assertEqual(s.encode(["Mar"], domain), [2])
        self.assertEqual(s.encode(["Jan", "Mar"], domain), [0, 2])

    def test_categorical_unknown_value(self):
        with self.assertRaises(DKitPlotException):
            scale.Categorical().encode(["z"], ["a"])

    def test_categorical_configures_fixed_ticks(self):
        fig, ax = plt.subplots()
        scale.Categorical("Month").configure(ax, "x", Theme(), ["Jan", "Feb"])
        self.assertIsInstance(ax.xaxis.get_major_locator(), FixedLocator)
        self.assertEqual(ax.get_xlabel(), "Month")
        self.assertEqual(ax.get_xlim(), (-0.5, 1.5))
        plt.close(fig)

    def test_categorical_formats_dates(self):
        """a categorical axis of dates is labelled with a strftime spec"""
        fig, ax = plt.subplots()
        s = scale.Categorical(format="%b %Y")
        s.configure(ax, "x", Theme(), [date(2026, 1, 31), date(2026, 2, 28)])
        labels = [t.get_text() for t in ax.get_xticklabels()]
        self.assertEqual(labels, ["Jan 2026", "Feb 2026"])
        plt.close(fig)

    def test_linear_format(self):
        fig, ax = plt.subplots()
        scale.Linear(format="{x:,.1f}").configure(ax, "y", Theme())
        self.assertIsInstance(ax.yaxis.get_major_formatter(), StrMethodFormatter)
        plt.close(fig)

    def test_linear_format_from_theme(self):
        fig, ax = plt.subplots()
        scale.Linear().configure(ax, "y", Theme(number_format="{x:,.0f}"))
        self.assertIsInstance(ax.yaxis.get_major_formatter(), StrMethodFormatter)
        plt.close(fig)

    def test_linear_leaves_default_formatter_alone(self):
        """no format anywhere means matplotlib keeps picking the decimals"""
        fig, ax = plt.subplots()
        before = ax.yaxis.get_major_formatter()
        scale.Linear().configure(ax, "y", Theme())
        self.assertIs(ax.yaxis.get_major_formatter(), before)
        plt.close(fig)

    def test_percent(self):
        fig, ax = plt.subplots()
        scale.Percent().configure(ax, "y", Theme())
        self.assertIsInstance(ax.yaxis.get_major_formatter(), PercentFormatter)
        plt.close(fig)

    def test_log(self):
        fig, ax = plt.subplots()
        scale.Log().configure(ax, "y", Theme())
        self.assertEqual(ax.get_yscale(), "log")
        plt.close(fig)

    def test_time_uses_date_locator(self):
        fig, ax = plt.subplots()
        scale.Time().configure(ax, "x", Theme())
        self.assertIsInstance(ax.xaxis.get_major_locator(), AutoDateLocator)
        plt.close(fig)

    def test_ticks_false_keeps_label(self):
        """replaces XAxis(defeat=True): no ticks, label still drawn"""
        fig, ax = plt.subplots()
        scale.Linear("Value", ticks=False).configure(ax, "x", Theme())
        self.assertEqual(list(ax.get_xticks()), [])
        self.assertEqual(ax.get_xlabel(), "Value")
        plt.close(fig)

    def test_limits(self):
        fig, ax = plt.subplots()
        scale.Linear(limits=(0, 10)).configure(ax, "y", Theme())
        self.assertEqual(ax.get_ylim(), (0.0, 10.0))
        plt.close(fig)

    def test_rotation(self):
        fig, ax = plt.subplots()
        scale.Categorical(rotation=80).configure(ax, "x", Theme(), ["a", "b"])
        self.assertAlmostEqual(ax.get_xticklabels()[0].get_rotation(), 80)
        plt.close(fig)

    def test_grid_override(self):
        fig, ax = plt.subplots()
        scale.Linear(grid=True).configure(ax, "y", Theme())
        self.assertTrue(ax.yaxis.get_gridlines()[0].get_visible())
        plt.close(fig)

    def test_invalid_axis(self):
        fig, ax = plt.subplots()
        with self.assertRaises(DKitPlotException):
            scale.Linear().configure(ax, "z", Theme())
        plt.close(fig)

    def test_replace_is_immutable(self):
        base = scale.Linear("Value")
        other = base.replace(label="Other")
        self.assertEqual(base.label, "Value")
        self.assertEqual(other.label, "Other")


class TestPlotComposition(TestCase):

    def test_layers_are_values(self):
        a, b = Line("a"), Line("b")
        p = Plot(a, b)
        self.assertEqual(p.layers, (a, b))

    def test_star_args_and_list(self):
        layers = [Line("a"), Line("b")]
        self.assertEqual(len(Plot(*layers).layers), 2)
        self.assertEqual(len(Plot(layers).layers), 2)

    def test_add_does_not_mutate(self):
        """the defect that motivated dropping the + operator"""
        base = Plot(Line("base"), title="base")
        b = base.add(Line("b"))
        c = base.add(Line("c"))
        self.assertIsNot(base, b)
        self.assertEqual(len(base.layers), 1)
        self.assertEqual(len(b.layers), 2)
        self.assertEqual([la.label for la in c.layers], ["base", "c"])

    def test_replace_does_not_mutate(self):
        base = Plot(title="one")
        other = base.replace(title="two")
        self.assertEqual(base.title, "one")
        self.assertEqual(other.title, "two")
        self.assertEqual(other.layers, base.layers)

    def test_replace_unknown_option(self):
        with self.assertRaises(DKitPlotException):
            Plot().replace(nope=1)

    def test_rejects_non_layer(self):
        with self.assertRaises(DKitPlotException):
            Plot("not a layer")

    def test_rejects_bad_axis(self):
        with self.assertRaises(DKitPlotException):
            Plot(Line("a", axis="middle"))

    def test_default_scales(self):
        p = Plot()
        self.assertIsInstance(p.x, scale.Linear)
        self.assertIsInstance(p.y, scale.Linear)


class TestPlotRender(TestCase):

    def test_empty_plot_renders_under_every_style(self):
        """an empty Plot must still produce styled, labelled axes"""
        for name in bundled_styles():
            p = Plot(x=scale.Linear("X"), y=scale.Linear("Y"),
                     title="Empty", theme=Theme(rc=name))
            fig = p.render(DATA)
            ax = fig.axes[0]
            # the style sheets set axes.titlelocation, so the title is not
            # necessarily the centre one
            titles = [ax.get_title(loc=loc) for loc in ("left", "center", "right")]
            self.assertIn("Empty", titles, f"no title under {name}")
            self.assertEqual(ax.get_xlabel(), "X")
            self.assertEqual(ax.get_ylabel(), "Y")
            plt.close(fig)

    def test_rcparams_unchanged_after_render(self):
        before = dict(mpl.rcParams)
        Plot(Line("a", y="sales"), theme="dkit-dark").render(DATA)
        after = dict(mpl.rcParams)
        changed = [k for k in before if repr(before[k]) != repr(after[k])]
        self.assertEqual(changed, [])
        plt.close("all")

    def test_spec_is_reusable(self):
        """rendering twice produces independent figures from one spec"""
        p = Plot(Line("a", x="month", y="sales"), x=scale.Categorical())
        fig1 = p.render(DATA)
        fig2 = p.render(DATA[:2])
        self.assertIsNot(fig1, fig2)
        self.assertEqual(len(fig1.axes[0].lines[0].get_xdata()), 3)
        self.assertEqual(len(fig2.axes[0].lines[0].get_xdata()), 2)
        plt.close("all")

    def test_render_into_supplied_axes(self):
        fig, ax = plt.subplots()
        returned = Plot(Line("a", y="sales")).render(DATA, ax=ax)
        self.assertIs(returned, fig)
        self.assertEqual(len(fig.axes), 1)
        self.assertEqual(len(ax.lines), 1)
        plt.close(fig)

    def test_implicit_x_index(self):
        p = Plot(Line("a", y="sales"))
        fig = p.render(DATA)
        self.assertEqual(list(fig.axes[0].lines[0].get_xdata()), [0, 1, 2])
        plt.close(fig)

    def test_categorical_domain_shared_across_layers(self):
        """each layer's own where must not shift the category positions"""
        p = Plot(
            Line("all", x="month", y="sales"),
            Line("high", x="month", y="sales", where="${sales} > 95"),
            x=scale.Categorical("Month"),
        )
        fig = p.render(DATA)
        ax = fig.axes[0]
        self.assertEqual(list(ax.lines[0].get_xdata()), [0, 1, 2])
        self.assertEqual(list(ax.lines[1].get_xdata()), [0, 1])
        labels = [t.get_text() for t in ax.get_xticklabels()]
        self.assertEqual(labels, ["Jan", "Feb", "Mar"])
        plt.close(fig)

    def test_layer_where_filters_that_layer_only(self):
        low = Line("low", y="sales", where="${sales} < 95")
        p = Plot(Line("all", y="sales"), low)
        fig = p.render(DATA)
        self.assertEqual(len(fig.axes[0].lines[0].get_ydata()), 3)
        self.assertEqual(list(fig.axes[0].lines[1].get_ydata()), [90.0])
        plt.close(fig)

    def test_plot_where_filters_every_layer(self):
        p = Plot(Line("a", y="sales"), Line("b", y="cost"), where="${sales} > 95")
        fig = p.render(DATA)
        for line in fig.axes[0].lines:
            self.assertEqual(len(line.get_ydata()), 2)
        plt.close(fig)

    def test_real_time_axis(self):
        """the case dkit.plot cannot express: genuine datetime coordinates"""
        p = Plot(Line("a", x="day", y="sales"), x=scale.Time("Date"))
        fig = p.render(DATA)
        ax = fig.axes[0]
        self.assertIsInstance(ax.xaxis.get_major_locator(), AutoDateLocator)
        # 2026 is ~20 000 days after the matplotlib epoch, not position 0..2
        self.assertGreater(ax.get_xlim()[0], 19000)
        plt.close(fig)

    def test_right_axis_creates_twin(self):
        p = Plot(
            Line("left", y="sales"),
            Line("right", y="cost", axis="right"),
            y_right=scale.Percent("Share"),
        )
        fig = p.render(DATA)
        self.assertEqual(len(fig.axes), 2)
        left, right = fig.axes
        self.assertEqual(len(left.lines), 1)
        self.assertEqual(len(right.lines), 1)
        self.assertEqual(right.get_ylabel(), "Share")
        self.assertTrue(right.spines["right"].get_visible())
        plt.close(fig)

    def test_no_twin_when_not_needed(self):
        fig = Plot(Line("a", y="sales")).render(DATA)
        self.assertEqual(len(fig.axes), 1)
        plt.close(fig)

    def test_legend_auto_needs_two_layers(self):
        one = Plot(Line("only", y="sales")).render(DATA)
        self.assertIsNone(one.axes[0].get_legend())
        two = Plot(Line("a", y="sales"), Line("b", y="cost")).render(DATA)
        self.assertIsNotNone(two.axes[0].get_legend())
        plt.close("all")

    def test_legend_forced(self):
        fig = Plot(Line("only", y="sales"), legend=True).render(DATA)
        legend = fig.axes[0].get_legend()
        self.assertEqual([t.get_text() for t in legend.get_texts()], ["only"])
        plt.close(fig)

    def test_legend_suppressed(self):
        fig = Plot(Line("a", y="sales"), Line("b", y="cost"), legend=False).render(DATA)
        self.assertIsNone(fig.axes[0].get_legend())
        plt.close(fig)

    def test_legend_includes_right_axis(self):
        p = Plot(
            Line("left", y="sales"),
            Line("right", y="cost", axis="right"),
        )
        fig = p.render(DATA)
        legend = fig.axes[0].get_legend()
        self.assertEqual([t.get_text() for t in legend.get_texts()], ["left", "right"])
        plt.close(fig)

    def test_default_colors_come_from_the_cycle(self):
        theme = Theme(rc="dkit-light", categorical=("#111111", "#222222"))
        p = Plot(Line("a", y="sales"), Line("b", y="cost"), theme=theme)
        fig = p.render(DATA)
        colors = [la.get_color() for la in fig.axes[0].lines]
        self.assertEqual(colors, ["#111111", "#222222"])
        plt.close(fig)

    def test_semantic_color(self):
        theme = Theme(negative="#ff0000")
        fig = Plot(Line("a", y="sales", color="negative"), theme=theme).render(DATA)
        self.assertEqual(fig.axes[0].lines[0].get_color(), "#ff0000")
        plt.close(fig)

    def test_literal_color(self):
        fig = Plot(Line("a", y="sales", color="#123456")).render(DATA)
        self.assertEqual(fig.axes[0].lines[0].get_color(), "#123456")
        plt.close(fig)

    def test_width_and_height_override_theme(self):
        fig = Plot(width=10, height=5).render(DATA)
        self.assertAlmostEqual(fig.get_figwidth(), 10 * 0.393701, places=4)
        self.assertAlmostEqual(fig.get_figheight(), 5 * 0.393701, places=4)
        plt.close(fig)

    def test_context_carries_theme_and_scales(self):
        layer = Line("a", x="month", y="sales")
        Plot(layer, x=scale.Categorical()).render(DATA)
        ctx = layer.contexts[0]
        self.assertIsInstance(ctx.theme, Theme)
        self.assertIsInstance(ctx.x, scale.Categorical)
        self.assertEqual(ctx.x_domain, ["Jan", "Feb", "Mar"])
        self.assertEqual(ctx.index, 0)
        plt.close("all")

    def test_save(self):
        target = "test_plot2_core_output.svg"
        try:
            Plot(Line("a", y="sales"), title="Saved").save(DATA, target)
            self.assertTrue(os.path.exists(target))
            self.assertGreater(os.path.getsize(target), 0)
        finally:
            if os.path.exists(target):
                os.remove(target)

    def test_render_accepts_a_frame(self):
        fig = Plot(Line("a", y="sales")).render(Frame(DATA))
        self.assertEqual(len(fig.axes[0].lines[0].get_ydata()), 3)
        plt.close(fig)


class TestSaveFigure(TestCase):
    """save_figure exists because savefig.* rcParams are scoped to the theme"""

    def setUp(self):
        self.target = "test_plot2_save_figure_output.png"

    def tearDown(self):
        if os.path.exists(self.target):
            os.remove(self.target)
        plt.close("all")

    def test_writes_the_file(self):
        fig = Plot(Line("a", y="sales")).render(DATA)
        save_figure(fig, self.target)
        self.assertGreater(os.path.getsize(self.target), 0)

    def test_closes_the_figure(self):
        fig = Plot(Line("a", y="sales")).render(DATA)
        save_figure(fig, self.target)
        self.assertNotIn(fig.number, plt.get_fignums())

    def test_close_false_keeps_it(self):
        fig = Plot(Line("a", y="sales")).render(DATA)
        save_figure(fig, self.target, close=False)
        self.assertIn(fig.number, plt.get_fignums())

    def test_applies_the_style_sheet_bbox(self):
        """the whole point: a bare fig.savefig would clip the axis labels"""
        fig = Plot(Line("a", y="sales"),
                   y=scale.Linear("A deliberately long axis label")).render(DATA)
        save_figure(fig, self.target, close=False)
        tight = os.path.getsize(self.target)
        # savefig.bbox only applies inside the context, so saving outside one
        # produces a figure-sized image instead of a cropped one
        fig.savefig(self.target)
        self.assertNotEqual(os.path.getsize(self.target), tight)

    def test_kwargs_win_over_the_rcparams(self):
        fig = Plot(Line("a", y="sales")).render(DATA)
        save_figure(fig, self.target, dpi=40)
        with open(self.target, "rb") as infile:
            # PNG width is a big endian long at byte 16
            width = int.from_bytes(infile.read(20)[16:20], "big")
        self.assertLess(width, 400)

    def test_rcparams_are_not_leaked(self):
        before = mpl.rcParams["savefig.bbox"]
        save_figure(Plot(Line("a", y="sales")).render(DATA), self.target)
        self.assertEqual(mpl.rcParams["savefig.bbox"], before)

    def test_unknown_theme(self):
        with self.assertRaises(DKitPlotException):
            save_figure(plt.figure(), self.target, theme="no-such-theme")


if __name__ == "__main__":
    main()
