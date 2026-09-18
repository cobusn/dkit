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
"""End-to-end checks for the runnable Doc2 style-pack examples."""

from email import message_from_string
from importlib.metadata import Distribution
from pathlib import Path
import sys

import pytest

from dkit.doc2 import builder
from dkit.stylepack.model import StylePackReference
from dkit.stylepack.registry import StyleRegistry


EXAMPLE_ROOT = Path(__file__).parents[1] / "examples" / "doc2_stylepacks"
sys.path.insert(0, str(EXAMPLE_ROOT))
from build_all import StylePackScenarioBuilder


@pytest.mark.skipif(
    builder.shutil.which("pdflatex") is None,
    reason="pdflatex is not installed",
)
def test_build_all_exercises_api_cli_report_and_email(tmp_path, monkeypatch):
    """Each documented scenario renders every dkit-blue output variant."""
    distribution = Distribution.from_name("libdkit")
    config_path = tmp_path / "dk.ini"
    StyleRegistry(config_path).register(
        StylePackReference(
            name="dkit-blue",
            distribution="libdkit",
            manifest="dkit/stylepacks/dkit_blue/style.yaml",
        ),
        distribution=distribution,
    )
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / "matplotlib"))

    output = tmp_path / "output"
    results = StylePackScenarioBuilder(
        "dkit-blue",
        output,
        config_path,
    ).build_all()

    expected = {
        "dkit_blue_api_chart.png",
        "dkit_blue_api_rl.pdf",
        "dkit_blue_api_latex.pdf",
        "dkit_blue_api_docx.docx",
        "dkit_blue_api_html.html",
        "dkit_blue_email.html",
        "dkit_blue_email.eml",
        "dkit_blue_doc_rl.pdf",
        "dkit_blue_doc_latex.pdf",
        "dkit_blue_doc_docx.docx",
        "dkit_blue_doc_html.html",
        "dkit_blue_report_rl.pdf",
        "dkit_blue_report_latex.pdf",
        "dkit_blue_report_docx.docx",
        "dkit_blue_report_html.html",
    }
    assert {path.name for path in results} == expected
    assert all(path.is_file() and path.stat().st_size > 0 for path in results)

    email = message_from_string(
        (output / "dkit_blue_email.eml").read_text(encoding="utf-8")
    )
    parts = list(email.walk())
    assert email.get_content_type() == "multipart/alternative"
    assert {part.get_content_type() for part in parts} >= {
        "text/plain",
        "text/html",
        "image/png",
    }
    html = (output / "dkit_blue_email.html").read_text(encoding="utf-8")
    assert "style=" in html
    assert "cid:" in html

    for scenario in ("api", "report"):
        source = output / f"dkit_blue_{scenario}_latex.tex"
        assert r"\rowcolor{tableheader}" in source.read_text(encoding="utf-8")
