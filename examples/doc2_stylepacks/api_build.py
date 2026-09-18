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
"""Create a styled document directly through the Doc2 Python API."""

from argparse import ArgumentParser
from pathlib import Path

from dkit.doc2 import builder
from dkit.doc2 import document as doc
from dkit.stylepack.matplotlib import matplotlib_theme
from dkit.stylepack.model import StylePack
from dkit.stylepack.registry import StyleRegistry
from dkit.plot2 import Plot, geom, scale


SAMPLE_ROWS = [
    {"service": "Data platform", "status": "on track", "completion": 92},
    {"service": "Document automation", "status": "on track", "completion": 84},
    {"service": "Customer migration", "status": "at risk", "completion": 68},
]


class ApiDocument(doc.Document):
    """Canonical API document whose template owns content placement.

    Args:
        style_pack: validated style pack used by the document and its plots.
        chart_path: PNG path written by the decorated chart method.
    """

    def __init__(self, style_pack: StylePack, chart_path: Path):
        super().__init__(
            title="Delivery overview",
            sub_title="Style-pack API example",
            author="Document Automation Team",
            contact="reports@example.test",
            version="1.0",
        )
        self.chart_path = chart_path
        self.plot_theme = None
        if style_pack.manifest.formats.matplotlib is not None:
            self.plot_theme = matplotlib_theme(style_pack, "print")

    @doc.wrap_json
    def table(self) -> doc.Table:
        """Return the API-generated table for the Jinja template.

        Returns:
            A canonical Doc2 table with heading and numeric formatting.
        """
        return doc.Table(
            SAMPLE_ROWS,
            [
                doc.Column("service", "Service", width=6),
                doc.Column("status", "Status", width=4),
                doc.Column(
                    "completion",
                    "Completion",
                    width=3,
                    align="right",
                    format_="{0}%",
                ),
            ],
        )

    @doc.wrap_matplotlib(
        filename=lambda document: str(document.chart_path),
        width=16,
        height=7,
    )
    def chart(self):
        """Create the API-generated Plot2 chart under the document style."""
        return Plot(
            geom.Bar("Completion", x="service", y="completion"),
            x=scale.Categorical("Service"),
            y=scale.Linear("Completion (%)", limits=(0, 100)),
            title="Delivery completion",
            width=16,
            height=7,
        ).render(SAMPLE_ROWS)

    def build(self):
        """Build the document through its Jinja template."""
        self.add_template(
            """# Summary

This document is assembled directly through the **Doc2 API**. It includes
a canonical table and a matplotlib chart so a selected style is applied to
both document and visual content.

## Delivery status

{{ report.table() }}

## Completion trend

{{ report.chart() }}
""",
            report=self,
        )
        return self


class ApiDocumentBuilder:
    """Build a document through the document-level style API.

    Args:
        style_pack: validated pack used by the document.
        chart_path: PNG path written by the document's chart method.
    """

    def __init__(self, style_pack: StylePack, chart_path: Path):
        self.style_pack = style_pack
        self.chart_path = chart_path

    def build_document(self) -> doc.Document:
        """Create and populate the styled API document."""
        return ApiDocument(self.style_pack, self.chart_path).build()


def render_api_document(
    style_name: str,
    renderer: str,
    output: Path,
    config_path: str | Path = "~/.dk.ini",
) -> tuple[doc.Document, StylePack]:
    """Build one API-first styled output.

    Args:
        style_name: registered style-pack name.
        renderer: Doc2 renderer name.
        output: destination output path.
        config_path: style registration INI path.

    Returns:
        The rendered document and selected style pack.
    """
    style_pack = StyleRegistry(config_path).get(style_name)
    document = ApiDocumentBuilder(
        style_pack,
        output.with_name(f"{output.stem}_chart.png"),
    ).build_document()
    output.parent.mkdir(parents=True, exist_ok=True)
    builder.render_to_file(
        document,
        renderer,
        str(output),
        style_pack=style_pack,
    )
    return document, style_pack


def main(arguments: list[str] | None = None):
    """Run the API-first example from the command line."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("style", help="registered style-pack name")
    parser.add_argument(
        "--format",
        choices=sorted(builder.RENDERERS),
        default="reportlab",
        help="output renderer",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", default="~/.dk.ini")
    args = parser.parse_args(arguments)
    render_api_document(args.style, args.format, args.output, args.config)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
