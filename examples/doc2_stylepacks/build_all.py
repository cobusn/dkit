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
"""Build all Doc2 style-pack examples for one registered style."""

from argparse import ArgumentParser
from pathlib import Path
import os
import re
import shutil
import subprocess
import sys
import tempfile

import yaml

from dkit.doc2 import builder
from dkit.stylepack.registry import StyleRegistry
from dkit.utilities.smtp_helper import DocumentMessage

from api_build import ApiDocumentBuilder


HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
REPORT_PROJECT = HERE / "report_project"
MARKDOWN_DOCUMENT = HERE / "document.md"

RENDERER_SUFFIXES = {
    "reportlab": ("rl", ".pdf"),
    "latex": ("latex", ".pdf"),
    "docx": ("docx", ".docx"),
    "html": ("html", ".html"),
}


class StylePackScenarioBuilder:
    """Build every supported example scenario for one registered style.

    Args:
        style_name: registered style-pack name.
        output_dir: directory that receives reviewable outputs.
        config_path: INI file containing the input style registration.
    """

    def __init__(
        self,
        style_name: str,
        output_dir: Path,
        config_path: str | Path = "~/.dk.ini",
    ):
        self.style_name = style_name
        self.output_dir = output_dir.resolve()
        self.config_path = Path(config_path).expanduser()
        self.style_pack = StyleRegistry(self.config_path).get(style_name)
        self.prefix = self._filename_prefix(self.style_pack.manifest.id)

    def build_all(self) -> list[Path]:
        """Build API, one-shot Markdown, report-project, and email outputs.

        Returns:
            Paths to primary generated output files.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="dkit-style-example-") as temp:
            config_path = self._write_isolated_registration(Path(temp))
            environment = self._subprocess_environment(Path(temp))
            document, results = self._build_api_outputs()
            results.extend(self._build_email_outputs(document))
            results.extend(
                self._build_cli_document_outputs(config_path, environment)
            )
            results.extend(self._build_report_outputs(environment))
        return results

    def _build_api_outputs(self) -> tuple[object, list[Path]]:
        """Build all native document renderers from one API document."""
        chart_path = self.output_dir / f"{self.prefix}_api_chart.png"
        document = ApiDocumentBuilder(
            self.style_pack,
            chart_path,
        ).build_document()

        results = [chart_path]
        for renderer in self._supported_renderers():
            suffix, extension = RENDERER_SUFFIXES[renderer]
            output = self.output_dir / f"{self.prefix}_api_{suffix}{extension}"
            builder.render_to_file(
                document,
                renderer,
                str(output),
                style_pack=self.style_pack,
            )
            results.append(output)
        return document, results

    def _build_email_outputs(self, document) -> list[Path]:
        """Write inlined email HTML and a complete RFC 2822 message."""
        formats = self.style_pack.manifest.formats.html
        if formats is None:
            return []
        message = DocumentMessage(
            subject="Delivery overview",
            sender="reports@example.test",
            recipients=["reviewer@example.test"],
            document=document,
            css=str(self.style_pack.resource(formats.email_stylesheet)),
        )
        html_path = self.output_dir / f"{self.prefix}_email.html"
        eml_path = self.output_dir / f"{self.prefix}_email.eml"
        html_path.write_text(message.html_body or "", encoding="utf-8")
        eml_path.write_text(message.get_mime_content(), encoding="utf-8")
        return [html_path, eml_path]

    def _build_cli_document_outputs(
        self,
        config_path: Path,
        environment: dict[str, str],
    ) -> list[Path]:
        """Build the Markdown scenario through the public CLI."""
        results = []
        for renderer in self._supported_renderers():
            suffix, extension = RENDERER_SUFFIXES[renderer]
            output = self.output_dir / f"{self.prefix}_doc_{suffix}{extension}"
            self._run_dk(
                [
                    "build",
                    "doc",
                    "--config",
                    str(config_path),
                    "--style",
                    self.style_pack.manifest.id,
                    "--format",
                    renderer,
                    "--title",
                    "Markdown document",
                    "--author",
                    "Document Automation Team",
                    "--output",
                    str(output),
                    str(MARKDOWN_DOCUMENT),
                ],
                environment,
            )
            results.append(output)
        return results

    def _build_report_outputs(
        self,
        environment: dict[str, str],
    ) -> list[Path]:
        """Build isolated report-project copies through ``dk build report``."""
        results = []
        for renderer in self._supported_renderers():
            suffix, extension = RENDERER_SUFFIXES[renderer]
            output = self.output_dir / (
                f"{self.prefix}_report_{suffix}{extension}"
            )
            with tempfile.TemporaryDirectory(
                prefix="dkit-style-report-",
            ) as temp:
                project = Path(temp) / "report_project"
                shutil.copytree(REPORT_PROJECT, project)
                self._configure_report(
                    project / "report.yaml",
                    renderer,
                    output,
                )
                self._run_dk(
                    ["build", "report", "--report", "report.yaml"],
                    environment,
                    cwd=project,
                )
            results.append(output)
        return results

    def _write_isolated_registration(self, temporary_root: Path) -> Path:
        """Register the already-validated pack into a private DK INI file."""
        config_path = temporary_root / "home" / ".dk.ini"
        StyleRegistry(config_path).register(
            self.style_pack.reference,
            distribution=self.style_pack.distribution,
        )
        return config_path

    def _subprocess_environment(self, temporary_root: Path) -> dict[str, str]:
        """Return an environment with private style registration state."""
        home = temporary_root / "home"
        home.mkdir(exist_ok=True)
        environment = os.environ.copy()
        environment["HOME"] = str(home)
        python_path = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = os.pathsep.join(
            value for value in (str(PROJECT_ROOT), python_path) if value
        )
        return environment

    def _configure_report(
        self,
        report_path: Path,
        renderer: str,
        output: Path,
    ):
        """Set one temporary report-project renderer, style, and output path."""
        definition = yaml.safe_load(report_path.read_text(encoding="utf-8"))
        configuration = definition["configuration"]
        configuration["renderer"] = renderer
        configuration["style"] = self.style_pack.manifest.id
        configuration["output"] = str(output)
        report_path.write_text(
            yaml.safe_dump(definition, sort_keys=False),
            encoding="utf-8",
        )

    def _supported_renderers(self) -> list[str]:
        """Return renderer names for which the selected pack supplies assets."""
        formats = self.style_pack.manifest.formats
        return [
            renderer
            for renderer in RENDERER_SUFFIXES
            if getattr(formats, renderer) is not None
        ]

    def _run_dk(
        self,
        arguments: list[str],
        environment: dict[str, str],
        cwd: Path | None = None,
    ):
        """Run the current interpreter's ``dk`` CLI and surface failures."""
        subprocess.run(
            [sys.executable, "-m", "lib_dk.dk", *arguments],
            check=True,
            cwd=cwd,
            env=environment,
        )

    @staticmethod
    def _filename_prefix(style_name: str) -> str:
        """Return a portable filename prefix from a registered style name."""
        return re.sub(r"[^a-z0-9]+", "_", style_name.lower()).strip("_")


def main(arguments: list[str] | None = None):
    """Run every style-pack scenario from the command line."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("style", help="registered style-pack name")
    parser.add_argument("--output", type=Path, default=HERE / "output")
    parser.add_argument("--config", default="~/.dk.ini")
    args = parser.parse_args(arguments)
    outputs = StylePackScenarioBuilder(
        args.style,
        args.output,
        args.config,
    ).build_all()
    for output in outputs:
        print(f"wrote {output}")


if __name__ == "__main__":
    main()
