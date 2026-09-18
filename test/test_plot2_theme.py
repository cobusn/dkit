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

import sys; sys.path.insert(0, "..")  # noqa
from unittest import TestCase, main

import matplotlib
matplotlib.use("Agg")
import matplotlib as mpl              # noqa: E402
import matplotlib.style as mpl_style  # noqa: E402

from dkit.exceptions import DKitPlotException   # noqa: E402
from dkit.plot2 import (                        # noqa: E402
    Plot,
    Theme,
    bundled_styles,
    default_theme,
    get_theme,
    register_theme,
    resolve_style,
    set_default_theme,
    themes,
    unregister_theme,
)
from dkit.plot2.matplotlib_extra import TreeMap  # noqa: E402
from dkit.plot2.theme import CM_TO_INCH, DEFAULT_THEME  # noqa: E402


class TestBundledStyles(TestCase):
    """bundled .mplstyle files must exist and be valid matplotlib"""

    def test_bundled_styles_present(self):
        available = bundled_styles()
        for name in ("dkit-light", "dkit-dark", "dkit-console", "dkit-print"):
            self.assertIn(name, available)

    def test_styles_load(self):
        """every bundled style must parse: catches invalid rcParam keys"""
        for name in bundled_styles():
            with mpl_style.context(resolve_style(name)):
                pass

    def test_light_style_values(self):
        with mpl_style.context(resolve_style("dkit-light")):
            self.assertAlmostEqual(mpl.rcParams["axes.linewidth"], 0.5)
            self.assertFalse(mpl.rcParams["axes.spines.right"])
            colors = mpl.rcParams["axes.prop_cycle"].by_key()["color"]
            self.assertEqual(colors[0], "#05386E")

    def test_console_style_values(self):
        with mpl_style.context(resolve_style("dkit-console")):
            self.assertEqual(mpl.rcParams["figure.facecolor"], "#101010")
            self.assertEqual(mpl.rcParams["text.color"], "#7FEFFF")
            self.assertEqual(mpl.rcParams["axes.labelcolor"], "#00E5FF")
            self.assertEqual(mpl.rcParams["xtick.color"], "#66D9EF")
            self.assertEqual(mpl.rcParams["ytick.color"], "#66D9EF")
            colors = mpl.rcParams["axes.prop_cycle"].by_key()["color"]
            self.assertEqual(
                colors,
                ["#00CC33", "#FFBF00", "#FF2B00", "#00BFFF", "#7F7F7F"],
            )

    def test_layering(self):
        """later layers win"""
        spec = resolve_style(["dkit-light", "dkit-print", {"font.size": 7}])
        with mpl_style.context(spec):
            self.assertEqual(mpl.rcParams["font.size"], 7.0)
            self.assertEqual(mpl.rcParams["figure.dpi"], 300.0)
            self.assertFalse(mpl.rcParams["axes.grid"])


class TestBundledPalettes(TestCase):
    """invariants that make the semantic colours usable alongside the cycle"""

    def test_highlight_is_not_a_cycle_colour(self):
        """a highlight that repeats a series colour highlights nothing"""
        for name, theme in themes.items():
            cycle = [c.lower() for c in theme.cycle()]
            with self.subTest(theme=name):
                self.assertNotIn(theme.highlight.lower(), cycle)

    def test_semantic_colours_are_distinct(self):
        for name, theme in themes.items():
            semantic = [theme.positive, theme.negative, theme.neutral,
                        theme.highlight]
            with self.subTest(theme=name):
                self.assertEqual(len(set(c.lower() for c in semantic)), 4)

    def test_legend_frame_masks_the_data_behind_it(self):
        """frameon False plus loc 'best' gives a legend unreadable over data"""
        # tested through the themes, not the style files: only dkit-light sets
        # these, and the other two are layered on top of it
        for name, theme in themes.items():
            with self.subTest(theme=name), theme.context():
                self.assertTrue(mpl.rcParams["legend.frameon"])
                self.assertGreater(mpl.rcParams["legend.framealpha"], 0.5)
                self.assertEqual(mpl.rcParams["legend.edgecolor"], "none")


class TestResolveStyle(TestCase):

    def test_bundled_name(self):
        self.assertTrue(resolve_style("dkit-light").endswith("dkit-light.mplstyle"))

    def test_builtin_name_passthrough(self):
        self.assertEqual(resolve_style("ggplot"), "ggplot")

    def test_mapping(self):
        self.assertEqual(resolve_style({"font.size": 8}), {"font.size": 8})

    def test_invalid(self):
        with self.assertRaises(DKitPlotException):
            resolve_style(3.14)


class TestTheme(TestCase):

    def test_context_is_scoped(self):
        """rcParams must be restored after the context exits"""
        before = list(mpl.rcParams["figure.figsize"])
        with Theme(rc="dkit-light", width=16, height=7).context():
            inside = list(mpl.rcParams["figure.figsize"])
        self.assertAlmostEqual(inside[0], 16 * CM_TO_INCH)
        self.assertAlmostEqual(inside[1], 7 * CM_TO_INCH)
        self.assertEqual(before, list(mpl.rcParams["figure.figsize"]))

    def test_reset_makes_render_deterministic(self):
        """ambient rcParams must not leak into a theme"""
        with mpl_style.context({"axes.linewidth": 9.0}):
            with Theme(rc="dkit-light").context():
                self.assertAlmostEqual(mpl.rcParams["axes.linewidth"], 0.5)

    def test_categorical_overrides_prop_cycle(self):
        theme = Theme(rc="dkit-light", categorical=("#111111", "#222222"))
        with theme.context():
            colors = mpl.rcParams["axes.prop_cycle"].by_key()["color"]
        self.assertEqual(colors, ["#111111", "#222222"])

    def test_cycle_falls_back_to_rc(self):
        """with no categorical set, the cycle comes from the style sheet"""
        self.assertEqual(Theme(rc="dkit-light").cycle()[0], "#05386E")

    def test_color_wraps(self):
        theme = Theme(categorical=("a", "b", "c"))
        self.assertEqual(theme.color(0), "a")
        self.assertEqual(theme.color(4), "b")
        self.assertEqual(theme.colors(4), ["a", "b", "c", "a"])

    def test_signed(self):
        theme = Theme()
        self.assertEqual(theme.signed(1.0), theme.positive)
        self.assertEqual(theme.signed(-1.0), theme.negative)
        self.assertEqual(theme.signed(0), theme.neutral)

    def test_replace_is_immutable(self):
        base = Theme()
        other = base.replace(sequential="magma")
        self.assertEqual(other.sequential, "magma")
        self.assertEqual(base.sequential, "viridis")

    def test_get_cmap_roles(self):
        theme = Theme(sequential="viridis", diverging="RdYlGn_r")
        self.assertEqual(theme.get_cmap("sequential").name, "viridis")
        self.assertEqual(theme.get_cmap("diverging").name, "RdYlGn_r")
        self.assertEqual(theme.get_cmap("magma").name, "magma")

    def test_get_cmap_invalid(self):
        with self.assertRaises(DKitPlotException):
            Theme().get_cmap("not-a-colour-map")

    def test_map_colors(self):
        colors = Theme().map_colors([0, 5, 10])
        self.assertEqual(len(colors), 3)
        self.assertEqual(len(colors[0]), 4)          # RGBA
        self.assertNotEqual(colors[0], colors[-1])

    def test_map_colors_constant_series(self):
        """a series with no range must not divide by zero"""
        colors = Theme().map_colors([7, 7, 7])
        self.assertEqual(len(colors), 3)
        self.assertEqual(colors[0], colors[1])

    def test_map_colors_empty(self):
        self.assertEqual(Theme().map_colors([]), [])

    def test_figsize_none_when_unset(self):
        self.assertIsNone(Theme().figsize)


class TestGetTheme(TestCase):

    def test_default(self):
        self.assertIsInstance(get_theme(), Theme)

    def test_by_name(self):
        for name in themes:
            self.assertIs(get_theme(name), themes[name])

    def test_passthrough(self):
        theme = Theme(sequential="magma")
        self.assertIs(get_theme(theme), theme)

    def test_style_name_shorthand(self):
        self.assertEqual(get_theme("ggplot").rc, "ggplot")

    def test_unknown(self):
        with self.assertRaises(DKitPlotException):
            get_theme("no-such-theme")

    def test_unknown_names_what_is_available(self):
        """the message has to be actionable: two of the three sources are lists"""
        with self.assertRaises(DKitPlotException) as caught:
            get_theme("no-such-theme")
        message = str(caught.exception)
        self.assertIn("dkit-light", message)
        self.assertIn("dkit-print", message)

    def test_registry_wins_over_a_bundled_style(self):
        """resolution order: registered name first, so a project can shadow one"""
        theme = Theme(sequential="magma")
        register_theme("dkit-print", theme, replace=True)
        try:
            self.assertIs(get_theme("dkit-print"), theme)
        finally:
            register_theme("dkit-print", Theme(rc=["dkit-light", "dkit-print"]),
                           replace=True)

    def test_bundled_style_wins_over_a_matplotlib_builtin(self):
        """a name that is both resolves to ours, with the theme layer attached"""
        self.assertIn("dkit-light", bundled_styles())
        self.assertIs(get_theme("dkit-light"), themes["dkit-light"])


class TestRegisterTheme(TestCase):

    def tearDown(self):
        themes.pop("house", None)

    def test_register_then_resolve_by_name(self):
        theme = Theme(sequential="magma")
        register_theme("house", theme)
        self.assertIs(get_theme("house"), theme)

    def test_returns_the_theme(self):
        """so a register call can wrap the constructor expression"""
        theme = Theme()
        self.assertIs(register_theme("house", theme), theme)

    def test_collision_is_refused(self):
        """a repeated name is more often a typo than an intent to restyle"""
        register_theme("house", Theme())
        with self.assertRaises(DKitPlotException):
            register_theme("house", Theme(sequential="magma"))

    def test_collision_with_replace(self):
        register_theme("house", Theme())
        theme = Theme(sequential="magma")
        register_theme("house", theme, replace=True)
        self.assertIs(themes["house"], theme)

    def test_a_name_is_not_a_theme(self):
        """register_theme takes the object, not another spec to resolve"""
        with self.assertRaises(DKitPlotException):
            register_theme("house", "dkit-dark")

    def test_unregister_returns_the_theme(self):
        theme = register_theme("house", Theme())
        self.assertIs(unregister_theme("house"), theme)
        self.assertNotIn("house", themes)

    def test_unregister_unknown(self):
        with self.assertRaises(DKitPlotException):
            unregister_theme("no-such-theme")

    def test_the_current_default_cannot_be_unregistered(self):
        """it would fail later, at an unrelated render, rather than here"""
        register_theme("house", Theme())
        previous = set_default_theme("house")
        try:
            with self.assertRaises(DKitPlotException):
                unregister_theme("house")
        finally:
            set_default_theme(previous)


class TestDefaultTheme(TestCase):

    def setUp(self):
        self.previous = set_default_theme()

    def tearDown(self):
        set_default_theme(self.previous)
        themes.pop("house", None)

    def test_default_is_dkit_light(self):
        self.assertIs(default_theme(), themes["dkit-light"])

    def test_set_by_name(self):
        set_default_theme("dkit-dark")
        self.assertIs(default_theme(), themes["dkit-dark"])

    def test_get_theme_none_follows_the_default(self):
        set_default_theme("dkit-dark")
        self.assertIs(get_theme(None), themes["dkit-dark"])

    def test_set_to_an_instance(self):
        """a project should not have to register a theme to default to it"""
        theme = Theme(sequential="magma")
        set_default_theme(theme)
        self.assertIs(get_theme(), theme)

    def test_set_to_a_registered_name(self):
        theme = register_theme("house", Theme(sequential="magma"))
        set_default_theme("house")
        self.assertIs(get_theme(), theme)

    def test_returns_the_previous_spec(self):
        """what makes save and restore a single line"""
        set_default_theme("dkit-dark")
        self.assertEqual(set_default_theme("dkit-print"), "dkit-dark")

    def test_none_restores_the_builtin_default(self):
        set_default_theme("dkit-dark")
        set_default_theme(None)
        self.assertIs(default_theme(), themes[DEFAULT_THEME])

    def test_an_unknown_name_raises_at_the_call_site(self):
        """not at the first render, which could be anywhere"""
        with self.assertRaises(DKitPlotException):
            set_default_theme("no-such-theme")
        self.assertIs(default_theme(), themes["dkit-light"])

    def test_a_plot_with_no_theme_uses_the_default(self):
        """the point of the feature: not repeating theme= everywhere"""
        set_default_theme("dkit-dark")
        self.assertIs(Plot().get_theme(), themes["dkit-dark"])

    def test_a_standalone_class_with_no_theme_uses_the_default(self):
        set_default_theme("dkit-dark")
        self.assertIs(TreeMap()._get_theme(None), themes["dkit-dark"])

    def test_an_explicit_theme_still_wins(self):
        set_default_theme("dkit-dark")
        self.assertIs(Plot(theme="dkit-print").get_theme(), themes["dkit-print"])


if __name__ == "__main__":
    main()
