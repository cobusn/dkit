"""Tests for M3 style-pack selection in the ReportLab renderer."""

import shutil
from pathlib import Path

from pdfrw import PdfReader

from dkit.doc2 import builder
from dkit.doc2 import document as doc
from dkit.doc2.document import Document
from dkit.doc2.rl_renderer import RLRenderer
from dkit.doc2.rl_helper import TableHelper
from dkit.doc2.rl_stylepack import StylePackStyler
from dkit.stylepack.loader import StylePackLoader
from dkit.stylepack.model import StylePackReference
from lib_dk.build_module import BuildModule


FIXTURE_ROOT = Path(__file__).parent / "data" / "stylepack"
SOURCE_COVER = Path(__file__).parents[1] / "dkit" / "resources" / "background.pdf"


class FixtureDistribution:
    """Distribution-like object rooted at a temporary style fixture."""

    version = "0.1.0"

    def __init__(self, root=None):
        self.root = root or FIXTURE_ROOT

    def locate_file(self, relative_path):
        return self.root / relative_path


def load_pack(
    tmp_path, size="a4", orientation="portrait", table_style=None
):
    """Copy the fixture, make its cover renderable, and load it."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "stylepack"
    shutil.copytree(FIXTURE_ROOT, root)
    style_root = root / "blue"
    shutil.copyfile(SOURCE_COVER, style_root / "assets" / "cover.pdf")

    manifest_path = style_root / "style.yaml"
    manifest = manifest_path.read_text(encoding="utf-8")
    manifest = manifest.replace("size: a4", f"size: {size}")
    manifest = manifest.replace(
        "orientation: portrait", f"orientation: {orientation}"
    )
    if table_style is not None:
        manifest = manifest.replace(
            "    template: docx/template.docx",
            "    template: docx/template.docx\n"
            f"    table_style: {table_style}",
        )
    manifest_path.write_text(manifest, encoding="utf-8")

    reference = StylePackReference(
        name="dkit-blue",
        distribution="test-style-pack",
        manifest="blue/style.yaml",
    )
    return StylePackLoader().load(reference, FixtureDistribution(root))


def test_style_pack_maps_shared_tokens_to_reportlab(tmp_path):
    """The ReportLab adapter consumes manifest tokens and native layout."""
    pack = load_pack(tmp_path)
    styler = StylePackStyler(Document(title="Example"), pack)

    assert styler.page_size == (595.2755905511812, 841.8897637795277)
    assert styler["Heading1"].textColor.hexval() == "0x173a5e"
    assert styler["Normal"].textColor.hexval() == "0x173a5e"
    assert styler["Heading1"].fontName == "SourceSansPro-Bold"
    assert styler.local_style["reportlab"]["front_page"]["title_xy"] == [40, 500]


def test_reportlab_table_uses_normal_body_font_size(tmp_path):
    """Rendered table commands use the normal body font size."""
    pack = load_pack(tmp_path)
    styler = StylePackStyler(Document(title="Example"), pack)
    table = doc.Table(
        data=[{"value": "text"}],
        columns=[doc.Column("value", "Value")],
    )

    commands = TableHelper(
        table, styler.local_style
    ).table_style().getCommands()

    assert (
        "FONTSIZE",
        (0, 0),
        (-1, -1),
        styler["Normal"].fontSize,
    ) in commands


def test_reportlab_headings_stay_with_the_next_flowable():
    """Heading flowables request a page break with their following content."""
    renderer = RLRenderer(Document(title="Example"))
    heading = next(renderer.make(doc.Heading([doc.Str("Heading")], level=1)))

    assert heading.keepWithNext == 1


def test_style_pack_renders_all_supported_page_sizes_and_orientations(tmp_path):
    """Physical output dimensions follow the manifest page tokens."""
    expected = {
        "a4": (595.2756, 841.8898),
        "letter": (612.0, 792.0),
        "legal": (612.0, 1008.0),
        "a5": (419.5276, 595.2756),
    }
    for size, dimensions in expected.items():
        for orientation in ("portrait", "landscape"):
            pack = load_pack(tmp_path / f"{size}-{orientation}", size, orientation)
            output = tmp_path / f"{size}-{orientation}.pdf"
            document = Document(title="Example")
            builder.render_to_file(
                document,
                "reportlab",
                str(output),
                style_pack=pack,
            )
            box = [float(value) for value in PdfReader(str(output)).pages[0].MediaBox]
            actual = tuple(round(value, 4) for value in box[2:])
            wanted = dimensions
            if orientation == "landscape":
                wanted = (dimensions[1], dimensions[0])
            assert actual == wanted


def test_custom_styler_remains_supported_with_deprecation_warning():
    """Legacy dotted stylers still resolve while signalling migration."""
    import pytest

    with pytest.warns(DeprecationWarning, match="style pack"):
        assert builder.resolve_styler("dkit.doc2.rl_styles.DefaultStyler")


def test_build_doc_style_flag_overrides_doc_ini(tmp_path):
    """The explicit build flag has priority over the DOC style setting."""
    config_path = tmp_path / "dk.ini"
    config_path.write_text("[DOC]\nstyle = ini-style\n", encoding="utf-8")
    input_path = tmp_path / "input.md"
    input_path.write_text("# heading\n", encoding="utf-8")

    module = BuildModule([
        "doc",
        "--config",
        str(config_path),
        "--style",
        "cli-style",
        "--output",
        str(tmp_path / "output.pdf"),
        "--title",
        "title",
        str(input_path),
    ])

    assert module.doc_style == "cli-style"


def test_default_style_name_keeps_legacy_renderer_path():
    """The explicit legacy default does not require a registry entry."""
    assert builder.resolve_style("default") is None
