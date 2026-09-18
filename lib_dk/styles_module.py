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
"""Manage installed dkit style-pack registrations."""

import sys
from pathlib import Path

from dkit.doc2 import builder
from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.stylepack.matplotlib import matplotlib_theme

from dkit.stylepack.errors import StylePackError
from dkit.stylepack.model import StylePackReference
from dkit.stylepack.registry import StyleRegistry
from . import module, options


class StylesModule(module.MultiCommandModule):
    """Register and inspect declarative style packs."""

    @property
    def registry(self):
        """Return a registry for the selected configuration file."""
        return StyleRegistry(self.args.config_uri)

    def run(self):
        """Run a styles command and report failures through stderr."""
        try:
            super().run()
        except StylePackError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc

    def do_register(self):
        """register an installed style pack"""
        try:
            reference = StylePackReference(
                name=self.args.name.lower(),
                distribution=self.args.distribution,
                manifest=self.args.manifest,
            )
        except Exception as exc:
            raise StylePackError(
                f"invalid style registration '{self.args.name}'"
            ) from exc
        persisted = self.registry.register(
            reference, replace_existing=self.args.replace
        )
        self.print(
            f"registered style '{persisted.name}' in "
            f"{self.registry.repository.path}"
        )

    def do_list(self):
        """list registered styles"""
        names = self.registry.names()
        if self.args.explain:
            for name in names:
                if name in self.registry.builtins:
                    self.print(f"{name}: built-in")
                    continue
                reference = self.registry.reference(name)
                self.print(
                    f"{name}: {reference.distribution} "
                    f"{reference.distribution_version or 'unknown'}"
                )
        else:
            for name in names:
                self.print(name)

    def do_show(self):
        """show a registered style"""
        pack = self.registry.get(self.args.name)
        manifest = pack.manifest
        self.print(f"name: {manifest.id}")
        self.print(f"display name: {manifest.name}")
        self.print(f"page: {manifest.page.size} {manifest.page.orientation}")
        self.print(
            "formats: " + ", ".join(
                name for name, value in manifest.formats.model_dump().items()
                if value is not None
            )
        )

    def do_validate(self):
        """validate a registered style"""
        self.registry.get(self.args.name)
        self.print(f"valid style '{self.args.name.lower()}'")

    def do_refresh(self):
        """refresh a registered style fingerprint"""
        refreshed = self.registry.refresh(self.args.name)
        self.print(
            f"refreshed style '{refreshed.name}' in "
            f"{self.registry.repository.path}"
        )

    def do_unregister(self):
        """remove a style registration"""
        self.registry.unregister(self.args.name, force=self.args.force)
        self.print(f"unregistered style '{self.args.name.lower()}'")

    def do_preview(self):
        """render a cross-format preview of a registered style"""
        pack = self.registry.get(self.args.name)
        output = Path(self.args.output)
        output.mkdir(parents=True, exist_ok=True)
        document = doc.Document(
            title=f"{pack.manifest.name} preview",
            sub_title="cross-format style preview",
            author="dkit",
            contact="styles@example.invalid",
        )
        document.add_template(
            "# A styled document\n\n"
            "This preview exercises headings, body text, tables, and charts."
        )
        self._render_preview_charts(document, pack, output)

        builder.render_to_file(
            document, "reportlab", str(output / "reportlab.pdf"),
            style_pack=pack,
        )
        builder.render_to_file(
            document, "latex", str(output / "latex.pdf"),
            style_pack=pack,
        )
        builder.render_to_file(
            document, "docx", str(output / "document.docx"),
            style_pack=pack,
        )
        HtmlRenderer(document, style_pack=pack).render(
            str(output / "document.html")
        )
        email_html = HtmlRenderer(
            document, style_pack=pack, inline_images=True
        ).render_email_string()
        (output / "email.html").write_text(email_html, encoding="utf-8")
        self.print(f"created style preview in {output}")

    @staticmethod
    def _render_preview_charts(document, pack, output):
        """Create representative screen and print charts."""
        import matplotlib.pyplot as plt

        for variant in ("screen", "print"):
            theme = matplotlib_theme(pack, variant)
            path = output / f"chart-{variant}.png"
            with theme.context():
                figure, axes = plt.subplots()
                axes.bar(["A", "B", "C"], [4, 7, 5])
                axes.set_title(f"{variant.title()} chart")
                figure.savefig(path)
                plt.close(figure)
            document.add_element(
                doc.Image(
                    str(path),
                    title=f"{variant.title()} chart",
                    width=theme.width,
                    height=theme.height,
                )
            )

    def init_parser(self):
        """initialize the styles command parser"""
        self.init_sub_parser()

        parser = self.sub_parser.add_parser(
            "register", help=self.do_register.__doc__
        )
        options.add_option_config(parser)
        parser.add_argument("name")
        parser.add_argument("--distribution", required=True)
        parser.add_argument("--manifest", required=True)
        parser.add_argument("--replace", action="store_true")

        parser = self.sub_parser.add_parser("list", help=self.do_list.__doc__)
        options.add_option_config(parser)
        parser.add_argument("--explain", action="store_true")

        for command, method in (("show", self.do_show), ("validate", self.do_validate)):
            parser = self.sub_parser.add_parser(command, help=method.__doc__)
            options.add_option_config(parser)
            parser.add_argument("name")

        parser = self.sub_parser.add_parser(
            "refresh", help=self.do_refresh.__doc__
        )
        options.add_option_config(parser)
        parser.add_argument("name")

        parser = self.sub_parser.add_parser(
            "unregister", help=self.do_unregister.__doc__
        )
        options.add_option_config(parser)
        parser.add_argument("name")
        parser.add_argument("--force", action="store_true")

        parser = self.sub_parser.add_parser(
            "preview", help=self.do_preview.__doc__
        )
        options.add_option_config(parser)
        parser.add_argument("name")
        parser.add_argument("--output", required=True)

        super().parse_args()
