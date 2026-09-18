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
"""ReportLab adapter for declarative dkit style packs."""

from copy import deepcopy
from pathlib import Path

import yaml
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

from dkit.stylepack.model import StylePack

from . import document as doc
from .rl_styles import DefaultStyler


class StylePackStyler(DefaultStyler):
    """Apply a validated style pack to the ReportLab renderer.

    Args:
        document: document being rendered.
        style_pack: validated style pack selected by the registry.
    """

    def __init__(self, document: doc.Document, style_pack: StylePack):
        local_style = deepcopy(self.load_local_style(
            "dkit.resources", "rl_stylesheet.yaml"
        ))
        manifest = style_pack.manifest
        self._font_names = self._register_fonts(style_pack)
        local_style["page"].update({
            "size": manifest.page.size,
            "orientation": manifest.page.orientation,
            "left": manifest.page.left_margin_cm * 10,
            "right": manifest.page.right_margin_cm * 10,
            "top": manifest.page.top_margin_cm * 10,
            "bottom": manifest.page.bottom_margin_cm * 10,
        })

        reportlab = manifest.formats.reportlab
        if reportlab is None:
            raise ValueError(
                f"style '{manifest.id}' does not provide ReportLab resources"
            )
        layout = self._load_layout(style_pack.resource(reportlab.layout))
        local_style["reportlab"]["front_page"].update(layout)
        local_style["reportlab"]["front_page"].update({
            "text_color": manifest.colors.text,
            "title_color": manifest.colors.primary,
        })
        local_style["reportlab"]["styles"].update(
            self._style_updates(
                manifest.colors, manifest.typography, self._font_names
            )
        )
        local_style["table"].update({
            "heading_color": manifest.colors.table_heading_text,
            "heading_background": manifest.colors.table_heading_background,
            "font_color": manifest.colors.text,
        })
        super().__init__(
            document,
            local_style=local_style,
            background_path=style_pack.resource(reportlab.cover),
        )

    @staticmethod
    def _load_layout(path: Path) -> dict:
        """Load the safe, renderer-native layout mapping."""
        with path.open("rt", encoding="utf-8") as infile:
            layout = yaml.safe_load(infile) or {}
        if not isinstance(layout, dict):
            raise ValueError("ReportLab style layout must be a mapping")
        return layout.get("page", layout)

    @staticmethod
    def _register_fonts(style_pack: StylePack) -> dict[str, dict[str, str]]:
        """Register bundled TrueType faces and return their ReportLab names."""
        registered = {}
        for font in style_pack.manifest.fonts:
            names = {
                "normal": font.family,
                "bold": f"{font.family}-Bold",
                "italic": f"{font.family}-Italic",
                "bold_italic": f"{font.family}-BoldItalic",
            }
            paths = {
                "normal": font.regular,
                "bold": font.bold,
                "italic": font.italic,
                "bold_italic": font.bold_italic,
            }
            for face, resource_name in paths.items():
                if resource_name is not None:
                    pdfmetrics.registerFont(
                        TTFont(names[face], str(style_pack.resource(resource_name)))
                    )
            pdfmetrics.registerFontFamily(
                font.family,
                normal=names["normal"],
                bold=names["bold"] if paths["bold"] else names["normal"],
                italic=(
                    names["italic"]
                    if paths["italic"]
                    else names["normal"]
                ),
                boldItalic=(
                    names["bold_italic"]
                    if paths["bold_italic"]
                    else names["bold"]
                    if paths["bold"]
                    else names["normal"]
                ),
            )
            registered[font.family.casefold()] = names
        return registered

    @staticmethod
    def _style_updates(colors, typography, font_names) -> dict:
        """Build ReportLab style overrides from shared design tokens."""
        heading_font = StylePackStyler._font_name(
            typography.heading, "bold", font_names
        )
        body_font = StylePackStyler._font_name(
            typography.body, "normal", font_names
        )
        return {
            "Heading1": {
                "textColor": colors.primary,
                "fontName": heading_font,
            },
            "Heading2": {
                "textColor": colors.primary,
                "fontName": heading_font,
            },
            "Heading3": {
                "textColor": colors.accent,
                "fontName": body_font,
            },
            "Normal": {
                "textColor": colors.text,
                "fontName": body_font,
            },
            "BodyText": {
                "textColor": colors.text,
                "fontName": body_font,
            },
            "UnorderedList": {"textColor": colors.text},
            "OrderedList": {"textColor": colors.text},
        }

    @staticmethod
    def _font_name(family, face, font_names) -> str:
        """Resolve a manifest font family to a registered ReportLab face."""
        names = font_names.get(family.casefold())
        if names is not None:
            return names[face]
        if family.casefold() == "source sans pro":
            return {
                "normal": "SourceSansPro",
                "bold": "SourceSansPro-Bold",
            }[face]
        return family
