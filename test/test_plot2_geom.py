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
tests for the dkit.plot2 geoms
"""
import sys; sys.path.insert(0, "..")  # noqa
from datetime import date
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt                    # noqa: E402
from matplotlib.collections import PolyCollection   # noqa: E402
from matplotlib.dates import AutoDateLocator        # noqa: E402
from matplotlib.colors import to_hex                # noqa: E402
from matplotlib.offsetbox import AnchoredText       # noqa: E402

from dkit.exceptions import DKitPlotException       # noqa: E402
from dkit.plot2 import Plot, Theme, geom, scale     # noqa: E402


DATA = [
    {"month": "Jan", "sales": 100.0, "delta": 10.0, "lcl": 80.0, "ucl": 120.0,
     "day": date(2026, 1, 31), "base": 0.0},
    {"month": "Feb", "sales": 150.0, "delta": -20.0, "lcl": 80.0, "ucl": 120.0,
     "day": date(2026, 2, 28), "base": 100.0},
    {"month": "Mar", "sales": 90.0, "delta": 0.0, "lcl": 80.0, "ucl": 120.0,
     "day": date(2026, 3, 31), "base": 250.0},
]

THEME = Theme(rc="dkit-light", positive="#00ff00", negative="#ff0000",
              neutral="#888888", highlight="#0000ff",
              categorical=("#111111", "#222222", "#333333"))


def render(*layers, **options):
    """render layers under a known theme and return the primary axes"""
    options.setdefault("theme", THEME)
    fig = Plot(*layers, **options).render(DATA)
    return fig, fig.axes[0]


def hexes(rgba) -> list:
    """matplotlib returns colours as numpy RGBA rows: compare them as hex"""
    return [to_hex(row) for row in rgba]


class TestLine(TestCase):

    def test_draws_one_line(self):
        fig, ax = render(geom.Line("Sales", x="month", y="sales"),
                         x=scale.Categorical())
        self.assertEqual(len(ax.lines), 1)
        self.assertEqual(list(ax.lines[0].get_ydata()), [100.0, 150.0, 90.0])
        plt.close(fig)

    def test_style_and_width(self):
        fig, ax = render(geom.Line("a", y="sales", style="--", width=3.0,
                                   marker="o", marker_size=8))
        line = ax.lines[0]
        self.assertEqual(line.get_linestyle(), "--")
        self.assertAlmostEqual(line.get_linewidth(), 3.0)
        self.assertEqual(line.get_marker(), "o")
        self.assertAlmostEqual(line.get_markersize(), 8)
        plt.close(fig)

    def test_omitted_options_fall_through_to_the_style_sheet(self):
        """a geom must not overwrite rcParams with a hardcoded None default"""
        fig, ax = render(geom.Line("a", y="sales"))
        # dkit-light sets lines.linewidth to 1.25
        self.assertAlmostEqual(ax.lines[0].get_linewidth(), 1.25)
        plt.close(fig)

    def test_label_reaches_the_legend(self):
        fig, ax = render(geom.Line("Sales", y="sales"), legend=True)
        self.assertEqual(
            [t.get_text() for t in ax.get_legend().get_texts()], ["Sales"]
        )
        plt.close(fig)

    def test_unlabelled_layer_stays_out_of_the_legend(self):
        fig, ax = render(geom.Line(y="sales"), geom.Line("Named", y="lcl"),
                         legend=True)
        self.assertEqual(
            [t.get_text() for t in ax.get_legend().get_texts()], ["Named"]
        )
        plt.close(fig)

    def test_real_time_axis(self):
        fig, ax = render(geom.Line("a", x="day", y="sales"), x=scale.Time())
        self.assertIsInstance(ax.xaxis.get_major_locator(), AutoDateLocator)
        self.assertGreater(ax.get_xlim()[0], 19000)
        plt.close(fig)


class TestArea(TestCase):

    def test_fill_and_line(self):
        fig, ax = render(geom.Area("a", y="sales"))
        fills = [c for c in ax.collections if isinstance(c, PolyCollection)]
        self.assertEqual(len(fills), 1)
        self.assertEqual(len(ax.lines), 1)
        plt.close(fig)

    def test_line_can_be_suppressed(self):
        fig, ax = render(geom.Area("a", y="sales", line=False))
        self.assertEqual(len(ax.lines), 0)
        plt.close(fig)

    def test_label_on_the_line_not_the_fill(self):
        """a legend swatch at fill_alpha is too faint to read"""
        fig, ax = render(geom.Area("Sales", y="sales"), legend=True)
        handles, labels = ax.get_legend_handles_labels()
        self.assertEqual(labels, ["Sales"])
        self.assertIs(handles[0], ax.lines[0])
        plt.close(fig)

    def test_base_field_stacks(self):
        fig, ax = render(geom.Area("a", y="sales", base="base"))
        fills = [c for c in ax.collections if isinstance(c, PolyCollection)]
        self.assertEqual(len(fills), 1)
        plt.close(fig)


class TestBand(TestCase):

    def test_fill_between_two_fields(self):
        fig, ax = render(geom.Band("Limits", x="month", upper="ucl", lower="lcl"),
                         x=scale.Categorical())
        fills = [c for c in ax.collections if isinstance(c, PolyCollection)]
        self.assertEqual(len(fills), 1)
        self.assertEqual(len(ax.lines), 0)
        plt.close(fig)

    def test_edges(self):
        fig, ax = render(geom.Band("a", upper="ucl", lower="lcl", edges=True))
        self.assertEqual(len(ax.lines), 2)
        plt.close(fig)

    def test_label_on_the_patch(self):
        """a band is the patch, so the legend swatch should be the patch"""
        fig, ax = render(geom.Band("Limits", upper="ucl", lower="lcl"), legend=True)
        handles, labels = ax.get_legend_handles_labels()
        self.assertEqual(labels, ["Limits"])
        self.assertIsInstance(handles[0], PolyCollection)
        plt.close(fig)

    def test_default_alpha_is_translucent(self):
        fig, ax = render(geom.Band("a", upper="ucl", lower="lcl"))
        self.assertAlmostEqual(ax.collections[0].get_alpha(), 0.2)
        plt.close(fig)

    def test_contributes_no_y_domain(self):
        """a band has no single y field, so it cannot supply a y domain"""
        band = geom.Band("a", upper="ucl", lower="lcl")
        fig, ax = render(band, y=scale.Categorical())
        self.assertIsNone(band.y)
        plt.close(fig)


class TestStem(TestCase):

    def test_draws_stems(self):
        fig, ax = render(geom.Stem("a", y="sales"))
        self.assertEqual(len(ax.collections) + len(ax.lines), 3)  # markers, stems, base
        plt.close(fig)

    def test_theme_color_is_applied_to_the_artists(self):
        """stem takes format strings, so colour must be set after the fact"""
        fig, ax = render(geom.Stem("a", y="sales", color="highlight"))
        container = ax.containers[0]
        self.assertEqual(container.markerline.get_color(), THEME.highlight)
        plt.close(fig)

    def test_baseline_hidden_by_default(self):
        fig, ax = render(geom.Stem("a", y="sales"))
        self.assertFalse(ax.containers[0].baseline.get_visible())
        plt.close(fig)

    def test_baseline_shown(self):
        fig, ax = render(geom.Stem("a", y="sales", baseline=True))
        self.assertTrue(ax.containers[0].baseline.get_visible())
        plt.close(fig)


class TestBar(TestCase):

    def test_vertical(self):
        fig, ax = render(geom.Bar("a", x="month", y="sales"), x=scale.Categorical())
        patches = ax.containers[0].patches
        self.assertEqual(len(patches), 3)
        self.assertAlmostEqual(patches[0].get_height(), 100.0)
        self.assertAlmostEqual(patches[0].get_width(), 0.8)
        plt.close(fig)

    def test_horizontal_uses_x_as_the_value(self):
        """x is always the horizontal field, whichever way the bars point"""
        fig, ax = render(
            geom.Bar("a", x="sales", y="month", horizontal=True),
            x=scale.Linear("Sales"), y=scale.Categorical("Month"),
        )
        patches = ax.containers[0].patches
        self.assertAlmostEqual(patches[0].get_width(), 100.0)
        self.assertAlmostEqual(patches[0].get_height(), 0.8)
        labels = [t.get_text() for t in ax.get_yticklabels()]
        self.assertEqual(labels, ["Jan", "Feb", "Mar"])
        plt.close(fig)

    def test_signed_colors_by_sign(self):
        """replaces GeomDelta and its two overlapping bar series"""
        fig, ax = render(geom.Bar("a", y="delta", color="signed"))
        colors = hexes([p.get_facecolor() for p in ax.containers[0].patches])
        self.assertEqual(colors, [THEME.positive, THEME.negative, THEME.neutral])
        plt.close(fig)

    def test_signed_needs_per_row_values(self):
        with self.assertRaises(DKitPlotException):
            render(geom.Line("a", y="sales", color="signed"))

    def test_base_field_stacks(self):
        fig, ax = render(geom.Bar("a", y="sales", base="base"))
        patches = ax.containers[0].patches
        self.assertAlmostEqual(patches[1].get_y(), 100.0)
        plt.close(fig)

    def test_offset_and_width_group_bars(self):
        fig, ax = render(
            geom.Bar("a", x="month", y="sales", width=0.4, offset=-0.2),
            geom.Bar("b", x="month", y="ucl", width=0.4, offset=0.2),
            x=scale.Categorical(),
        )
        first = ax.containers[0].patches[0]
        second = ax.containers[1].patches[0]
        self.assertAlmostEqual(first.get_x() + first.get_width(), second.get_x())
        plt.close(fig)

    def test_edge(self):
        fig, ax = render(geom.Bar("a", y="sales", edge_color="#000000", edge_width=2))
        patch = ax.containers[0].patches[0]
        self.assertEqual(to_hex(patch.get_edgecolor()), "#000000")
        self.assertAlmostEqual(patch.get_linewidth(), 2)
        plt.close(fig)


class TestScatter(TestCase):

    def test_draws_points(self):
        fig, ax = render(geom.Scatter("a", x="month", y="sales"), x=scale.Categorical())
        self.assertEqual(len(ax.collections), 1)
        self.assertEqual(len(ax.collections[0].get_offsets()), 3)
        plt.close(fig)

    def test_where_filters_this_layer_only(self):
        fig, ax = render(
            geom.Line("all", y="sales"),
            geom.Scatter("high", y="sales", where="${sales} > 120"),
        )
        self.assertEqual(len(ax.lines[0].get_ydata()), 3)
        self.assertEqual(len(ax.collections[0].get_offsets()), 1)
        plt.close(fig)

    def test_size_from_a_field(self):
        fig, ax = render(geom.Scatter("a", y="sales", size="sales"))
        self.assertEqual(list(ax.collections[0].get_sizes()), [100.0, 150.0, 90.0])
        plt.close(fig)

    def test_size_constant(self):
        fig, ax = render(geom.Scatter("a", y="sales", size=30))
        self.assertEqual(list(ax.collections[0].get_sizes()), [30])
        plt.close(fig)

    def test_semantic_color(self):
        fig, ax = render(geom.Scatter("a", y="sales", color="negative"))
        self.assertEqual(hexes(ax.collections[0].get_facecolor()), [THEME.negative])
        plt.close(fig)


class TestReference(TestCase):

    def test_hline(self):
        fig, ax = render(geom.Line("a", y="sales"), geom.HLine(100.0))
        self.assertEqual(len(ax.lines), 2)
        self.assertEqual(list(ax.lines[1].get_ydata()), [100.0, 100.0])
        plt.close(fig)

    def test_vline(self):
        fig, ax = render(geom.Line("a", y="sales"), geom.VLine(1.0))
        self.assertEqual(list(ax.lines[1].get_xdata()), [1.0, 1.0])
        plt.close(fig)

    def test_default_color_is_neutral_not_the_cycle(self):
        """a reference line in series colour reads as data"""
        fig, ax = render(geom.HLine(100.0))
        self.assertEqual(ax.lines[0].get_color(), THEME.neutral)
        plt.close(fig)

    def test_default_style_is_dashed(self):
        fig, ax = render(geom.HLine(100.0))
        self.assertEqual(ax.lines[0].get_linestyle(), "--")
        plt.close(fig)

    def test_explicit_color(self):
        fig, ax = render(geom.HLine(100.0, color="highlight"))
        self.assertEqual(ax.lines[0].get_color(), THEME.highlight)
        plt.close(fig)

    def test_lines_contribute_no_domain(self):
        """a reference line must not add a phantom category"""
        fig, ax = render(
            geom.Bar("a", x="month", y="sales"),
            geom.HLine(100.0),
            x=scale.Categorical(),
        )
        self.assertEqual([t.get_text() for t in ax.get_xticklabels()],
                         ["Jan", "Feb", "Mar"])
        plt.close(fig)

    def test_label_reaches_the_legend(self):
        fig, ax = render(geom.HLine(100.0, label="Target"), legend=True)
        self.assertEqual(
            [t.get_text() for t in ax.get_legend().get_texts()], ["Target"]
        )
        plt.close(fig)


class TestText(TestCase):

    def test_anchored(self):
        fig, ax = render(geom.Text("Q1", location="upper right"))
        artists = [a for a in ax.artists if isinstance(a, AnchoredText)]
        self.assertEqual(len(artists), 1)
        self.assertEqual(artists[0].txt.get_text(), "Q1")
        plt.close(fig)

    def test_four_corners(self):
        """the quadrant plot's use case"""
        corners = ("upper left", "upper right", "lower left", "lower right")
        fig, ax = render(*[geom.Text(c, location=c) for c in corners])
        self.assertEqual(len([a for a in ax.artists if isinstance(a, AnchoredText)]), 4)
        plt.close(fig)

    def test_no_frame_by_default(self):
        fig, ax = render(geom.Text("Q1"))
        self.assertFalse(ax.artists[0].patch.get_visible())
        plt.close(fig)

    def test_size_and_color(self):
        fig, ax = render(geom.Text("Q1", size=14, color="#123456"))
        text = ax.artists[0].txt.get_children()[0]
        self.assertAlmostEqual(text.get_fontsize(), 14)
        self.assertEqual(text.get_color(), "#123456")
        plt.close(fig)

    def test_contributes_no_domain(self):
        fig, ax = render(
            geom.Bar("a", x="month", y="sales"),
            geom.Text("note"),
            x=scale.Categorical(),
        )
        self.assertEqual([t.get_text() for t in ax.get_xticklabels()],
                         ["Jan", "Feb", "Mar"])
        plt.close(fig)


class TestColorAndAxis(TestCase):

    def test_cycle_color_per_layer(self):
        fig, ax = render(geom.Line("a", y="sales"), geom.Line("b", y="ucl"))
        self.assertEqual([la.get_color() for la in ax.lines], ["#111111", "#222222"])
        plt.close(fig)

    def test_multi_artist_geom_uses_one_color(self):
        """Area draws a line and a fill: both must be the layer's colour"""
        fig, ax = render(geom.Line("a", y="sales"), geom.Area("b", y="ucl"))
        self.assertEqual(ax.lines[1].get_color(), "#222222")
        self.assertEqual(hexes(ax.collections[0].get_facecolor()), ["#222222"])
        plt.close(fig)

    def test_right_axis_is_available_to_any_geom(self):
        """not hardcoded inside one geom the way GeomCumulative was"""
        fig, ax = render(
            geom.Bar("left", y="sales"),
            geom.Line("right", y="delta", axis="right"),
            y_right=scale.Percent("Cumulative"),
        )
        self.assertEqual(len(fig.axes), 2)
        self.assertEqual(len(fig.axes[1].lines), 1)
        self.assertEqual(fig.axes[1].get_ylabel(), "Cumulative")
        plt.close(fig)

    def test_alpha(self):
        fig, ax = render(geom.Line("a", y="sales", alpha=0.5))
        self.assertAlmostEqual(ax.lines[0].get_alpha(), 0.5)
        plt.close(fig)

    def test_zorder(self):
        fig, ax = render(geom.Line("a", y="sales", zorder=9))
        self.assertEqual(ax.lines[0].get_zorder(), 9)
        plt.close(fig)

    def test_repr(self):
        self.assertIn("label='a'", repr(geom.Line("a", y="sales")))
        self.assertIn("100", repr(geom.HLine(100.0)))
        self.assertIn("'note'", repr(geom.Text("note")))


class TestLayeredPlots(TestCase):

    def test_control_chart_shape(self):
        """the four-layer composition from the spec renders as one figure"""
        p = Plot(
            geom.Band("Limits", x="day", upper="ucl", lower="lcl",
                      color="positive", alpha=0.2),
            geom.Line("Expected", x="day", y="lcl", color="neutral", style="--"),
            geom.Line("Actual", x="day", y="sales"),
            geom.Scatter("Out of control", x="day", y="sales", color="negative",
                         where="${sales} > ${ucl} | ${sales} < ${lcl}"),
            x=scale.Time("Date", rotation=80),
            y=scale.Linear("Value"),
            title="Control Chart",
            theme=THEME,
        )
        fig = p.render(DATA)
        ax = fig.axes[0]
        self.assertEqual(len(ax.lines), 2)
        self.assertEqual(len(ax.collections), 2)          # band fill + scatter
        labels = [t.get_text() for t in ax.get_legend().get_texts()]
        self.assertEqual(labels, ["Limits", "Expected", "Actual", "Out of control"])
        plt.close(fig)

    def test_spec_reuse_with_geoms(self):
        p = Plot(geom.Bar("a", x="month", y="sales"), x=scale.Categorical())
        first = p.render(DATA)
        second = p.render(DATA[:2])
        self.assertEqual(len(first.axes[0].containers[0].patches), 3)
        self.assertEqual(len(second.axes[0].containers[0].patches), 2)
        plt.close("all")


if __name__ == "__main__":
    main()
