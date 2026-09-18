"""Tests for style-pack previews and the packaged reference style."""

import pytest

from lib_dk import styles_module

from test_stylepack_docx import make_template
from test_stylepack_reportlab import load_pack


@pytest.mark.skipif(
    styles_module.builder.shutil.which("pdflatex") is None,
    reason="pdflatex is not installed",
)
def test_styles_preview_creates_cross_format_bundle(tmp_path, monkeypatch):
    """Preview produces each declared output from the canonical document."""
    pack = load_pack(tmp_path / "pack")
    make_template(pack)

    class Registry:
        """Minimal registry boundary for the preview command."""

        def get(self, name):
            assert name == "dkit-blue"
            return pack

    monkeypatch.setattr(styles_module, "StyleRegistry", lambda _path: Registry())
    output = tmp_path / "preview"
    styles_module.StylesModule([
        "preview",
        "dkit-blue",
        "--config",
        str(tmp_path / "dk.ini"),
        "--output",
        str(output),
    ]).run()

    expected = {
        "reportlab.pdf",
        "latex.pdf",
        "latex.tex",
        "document.docx",
        "document.html",
        "email.html",
        "chart-screen.png",
        "chart-print.png",
    }
    assert {path.name for path in output.iterdir()} >= expected
