"""Tests for isolated LaTeX style-pack integration."""

import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from pdfrw import PdfReader

from dkit.doc2 import builder
from dkit.doc2.document import Document
from dkit.doc2.latex_renderer import LatexRenderer

from test_stylepack_reportlab import load_pack


@pytest.mark.skipif(
    builder.shutil.which("pdflatex") is None,
    reason="pdflatex is not installed",
)
def test_registered_class_compiles_without_global_tex_installation(tmp_path):
    """The pack class is found through a build-local TEXINPUTS path."""
    pack = load_pack(tmp_path)
    output = tmp_path / "styled.pdf"
    document = Document(title="Styled report", author="Author")
    document.add_template("# A heading\n\nA paragraph.")

    builder.render_to_file(document, "latex", str(output), style_pack=pack)

    source = output.with_suffix(".tex").read_text(encoding="utf-8")
    assert r"\documentclass[11pt,a4paper]{dkit-blue}" in source
    assert output.is_file()
    assert PdfReader(str(output)).pages


@pytest.mark.parametrize(
    ("size", "orientation", "option"),
    [
        ("a4", "portrait", "a4paper"),
        ("letter", "portrait", "letterpaper"),
        ("legal", "portrait", "legalpaper"),
        ("a5", "portrait", "a5paper"),
        ("a4", "landscape", "a4paper,landscape"),
        ("a5", "landscape", "a5paper,landscape"),
    ],
)
def test_style_pack_emits_supported_page_options(
    tmp_path, size, orientation, option
):
    """Every supported shared page token reaches the LaTeX preamble."""
    pack = load_pack(tmp_path / f"{size}-{orientation}", size, orientation)
    renderer = LatexRenderer(Document(title="Example"), style_pack=pack)

    assert f"[11pt,{option}]" in renderer._tex_doc.preamble()
    assert "left=2.0cm" in renderer._tex_doc.preamble()
    assert "top=2.2cm" in renderer._tex_doc.preamble()


def test_latex_resources_are_child_process_only_and_cleaned_on_failure(
    tmp_path, monkeypatch
):
    """TEXINPUTS is extended for pdflatex and temp resources are removed."""
    pack = load_pack(tmp_path)
    output = tmp_path / "failed.pdf"
    captured = {}

    def fake_run(*args, **kwargs):
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=1, stdout="compiler failure")

    monkeypatch.setenv("TEXINPUTS", "/existing/tex/path")
    monkeypatch.setattr(builder.subprocess, "run", fake_run)

    with pytest.raises(builder.DKitApplicationException, match="pdflatex failed"):
        builder.render_to_file(
            Document(title="Example"),
            "latex",
            str(output),
            style_pack=pack,
        )

    texinputs = captured["env"]["TEXINPUTS"]
    materialised = Path(texinputs.split(os.pathsep)[0])
    assert "/existing/tex/path" in texinputs
    assert not materialised.exists()
    assert os.environ["TEXINPUTS"] == "/existing/tex/path"
    assert not output.exists()
