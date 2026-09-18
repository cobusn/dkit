"""Tests for style-pack plotting and HTML integration."""

import copy
from pathlib import Path

import matplotlib
import pytest
from matplotlib import font_manager

from dkit.doc2 import builder
from dkit.doc2 import document as doc
from dkit.doc2.document import Document
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.plot2 import Plot, geom, scale
from dkit.plot2.theme import Theme, get_theme
from dkit.stylepack.matplotlib import matplotlib_theme
from dkit.stylepack.loader import StylePackLoader
from dkit.stylepack.model import StylePack, StylePackReference

from test_stylepack_reportlab import load_pack


class BuiltinDistribution:
    """Distribution-like root for the shipped dkit-blue style pack."""

    version = "26.9.1"

    def locate_file(self, relative_path):
        """Return a built-in style-pack resource."""
        return Path(__file__).parents[1] / relative_path


def load_builtin_pack():
    """Load the dkit-blue pack from the repository source tree."""
    reference = StylePackReference(
        name="dkit-blue",
        distribution="libdkit",
        manifest="dkit/stylepacks/dkit_blue/style.yaml",
    )
    return StylePackLoader().load(reference, BuiltinDistribution())


def test_style_pack_creates_screen_and_print_themes(tmp_path):
    """Native variants and shared chart tokens reach plot2.Theme."""
    pack = load_pack(tmp_path)

    screen = matplotlib_theme(pack, "screen")
    printed = matplotlib_theme(pack, "print")

    assert isinstance(screen, Theme)
    assert screen.categorical == pack.manifest.charts.categorical
    assert screen.width == pack.manifest.charts.width_cm
    assert screen.height == pack.manifest.charts.height_cm
    assert str(pack.resource("matplotlib/screen.mplstyle")) in str(screen.rc)
    assert str(pack.resource("matplotlib/print.mplstyle")) in str(printed.rc)


def test_style_pack_theme_context_restores_matplotlib_state(tmp_path):
    """Applying a pack theme never mutates global matplotlib settings."""
    pack = load_pack(tmp_path)
    before = copy.deepcopy(dict(matplotlib.rcParams))
    theme = matplotlib_theme(pack)

    with theme.context():
        assert matplotlib.rcParams["axes.facecolor"] == "white"
        assert matplotlib.rcParams["font.family"] == ["Source Sans Pro"]

    assert dict(matplotlib.rcParams) == before


def test_builtin_style_pack_resolves_its_bundled_font():
    """The shipped dkit-blue pack registers Source Sans Pro with matplotlib."""
    pack = load_builtin_pack()
    theme = matplotlib_theme(pack)

    with theme.context():
        resolved = font_manager.findfont(
            "Source Sans Pro", fallback_to_default=False
        )

    assert Path(resolved) == pack.resource(
        pack.manifest.fonts[0].regular
    )


def test_html_style_pack_uses_css_geometry_and_bounded_chart_width(tmp_path):
    """Styled HTML contains the pack stylesheet and selected page geometry."""
    pack = load_pack(tmp_path)
    html = HtmlRenderer(Document(title="Example"), style_pack=pack).render_string()

    assert "--dkit-primary: #173A5E" in html
    assert "size: a4 portrait" in html
    assert "margin: 2.2cm 2.0cm 2.0cm 2.0cm" in html
    assert "max-width: 16.0cm" in html


def test_html_style_pack_uses_email_stylesheet(tmp_path):
    """Email rendering selects the concrete, inlineable email asset."""
    pack = load_pack(tmp_path)
    renderer = HtmlRenderer(Document(title="Example"), style_pack=pack)

    try:
        html = renderer.render_email_string()
    except ImportError:
        pytest.skip("premailer is not installed")

    assert "border-bottom:3px solid #2569A9" in html


def test_builder_scopes_style_pack_theme_around_document_build(tmp_path, monkeypatch):
    """Project code creating plots sees the selected theme only during build."""
    pack = load_pack(tmp_path)
    definition = builder.DocumentDefinition(
        info=builder.DocumentInfo(title="Example", contact=""),
        configuration=builder.DocumentConfiguration(
            renderer="html", style="dkit-blue", output="unused.html"
        ),
        templates=[],
        code={},
        data={},
        variables={},
    )
    instance = builder.Builder(definition)
    observed = []
    original = dict(matplotlib.rcParams)
    monkeypatch.setattr(builder, "resolve_style", lambda _name: pack)
    monkeypatch.setattr(builder, "render_to_file", lambda *args, **kwargs: None)

    def build_document():
        observed.append(matplotlib.rcParams["axes.facecolor"])
        return Document(title="Example")

    monkeypatch.setattr(instance, "build_document", build_document)
    instance.build()

    assert observed == ["white"]
    assert dict(matplotlib.rcParams) == original


def test_builder_makes_style_pack_theme_implicit_for_plot2(
    tmp_path, monkeypatch
):
    """Report code can use Plot without repeating the selected theme."""
    pack = load_pack(tmp_path)
    definition = builder.DocumentDefinition(
        info=builder.DocumentInfo(title="Example", contact=""),
        configuration=builder.DocumentConfiguration(
            renderer="html", style="dkit-blue", output="unused.html"
        ),
        templates=[],
        code={},
        data={},
        variables={},
    )
    instance = builder.Builder(definition)
    observed = []
    monkeypatch.setattr(builder, "resolve_style", lambda _name: pack)
    monkeypatch.setattr(builder, "render_to_file", lambda *args, **kwargs: None)

    def build_document():
        observed.append(get_theme(None).rc)
        return Document(title="Example")

    monkeypatch.setattr(instance, "build_document", build_document)
    instance.build()

    assert str(pack.resource("matplotlib/screen.mplstyle")) in str(
        observed[0]
    )


def test_wrap_matplotlib_activates_injected_plot2_theme(tmp_path):
    """A decorated Plot method inherits and restores its injected theme."""
    pack = load_pack(tmp_path)
    theme = matplotlib_theme(pack, "print")
    output = tmp_path / "chart.png"
    observed = []

    class ReportCode:
        plot_theme = theme

        @doc.wrap_matplotlib(filename=str(output), width=10, height=6)
        def chart(self):
            import matplotlib.pyplot as pyplot

            observed.append(get_theme(None) is theme)
            Plot(
                geom.Bar("Completion", x="service", y="completion"),
                x=scale.Categorical("Service"),
                y=scale.Linear("Completion (%)"),
            ).render([{"service": "A", "completion": 1}])
            return pyplot

    before = get_theme(None)
    ReportCode().chart()

    assert observed == [True]
    assert output.is_file()
    assert get_theme(None) is before
