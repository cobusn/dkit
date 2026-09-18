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
"""Typed, renderer-independent style-pack models."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .errors import StylePackResourceError


class StrictModel(BaseModel):
    """Base model that rejects unknown style configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class ColourTokens(StrictModel):
    """Semantic colours shared by document renderers and plots."""

    primary: str
    accent: str
    background: str
    surface: str
    text: str
    muted: str
    positive: str
    negative: str
    neutral: str
    highlight: str
    table_heading_background: str
    table_heading_text: str


class TypographyTokens(StrictModel):
    """Font-family roles shared by renderers."""

    heading: str
    body: str
    mono: str


class FontTokens(StrictModel):
    """Bundled font faces for renderer-specific registration."""

    family: str = Field(min_length=1)
    regular: str
    bold: str | None = None
    italic: str | None = None
    bold_italic: str | None = None


class PageTokens(StrictModel):
    """Physical page geometry."""

    size: Literal["a4", "letter", "legal", "a5"]
    orientation: Literal["portrait", "landscape"]
    left_margin_cm: float = Field(gt=0)
    right_margin_cm: float = Field(gt=0)
    top_margin_cm: float = Field(gt=0)
    bottom_margin_cm: float = Field(gt=0)


class ChartTokens(StrictModel):
    """Plot palettes and default dimensions."""

    categorical: tuple[str, ...] = Field(min_length=1)
    sequential: str
    diverging: str
    positive: str
    negative: str
    neutral: str
    highlight: str
    width_cm: float = Field(gt=0)
    height_cm: float = Field(gt=0)


class ReportLabFormat(StrictModel):
    """ReportLab resource references."""

    layout: str
    cover: str


class HtmlFormat(StrictModel):
    """HTML and email CSS resource references."""

    stylesheet: str
    email_stylesheet: str


class LatexFormat(StrictModel):
    """LaTeX class and resource references."""

    class_name: str = Field(alias="class")
    resources: str
    engine: Literal["pdflatex"]

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True
    )


class DocxFormat(StrictModel):
    """DOCX template resource reference."""

    template: str
    table_style: str | None = None


class MatplotlibFormat(StrictModel):
    """Screen and print matplotlib style references."""

    screen: str
    print: str


class FormatTokens(StrictModel):
    """Optional native renderer resource references."""

    reportlab: ReportLabFormat | None = None
    html: HtmlFormat | None = None
    latex: LatexFormat | None = None
    docx: DocxFormat | None = None
    matplotlib: MatplotlibFormat | None = None


class StyleManifest(StrictModel):
    """Validated version-one style-pack manifest."""

    schema_version: Literal[1]
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    name: str = Field(min_length=1)
    requires_dkit: str = Field(min_length=1)
    colors: ColourTokens
    typography: TypographyTokens
    fonts: tuple[FontTokens, ...] = ()
    page: PageTokens
    charts: ChartTokens
    formats: FormatTokens


class StylePackReference(StrictModel):
    """A registered name and its installed distribution resource."""

    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    distribution: str = Field(min_length=1)
    manifest: str = Field(min_length=1)
    fingerprint: str | None = None
    distribution_version: str | None = None


@dataclass(frozen=True)
class StylePack:
    """A validated style manifest rooted in an installed distribution.

    Args:
        reference: registration metadata used to load the pack.
        manifest: validated manifest data.
        distribution: owning distribution metadata object.
        root: directory containing the manifest and native resources.
    """

    reference: StylePackReference
    manifest: StyleManifest
    distribution: Any
    root: Path

    def resource(self, relative_path: str) -> Path:
        """Return an existing resource contained by this style root.

        Args:
            relative_path: POSIX-style path relative to the style root.

        Returns:
            Absolute path to the resource.

        Raises:
            StylePackResourceError: if the path is invalid or unavailable.
        """
        parts = _resource_parts(relative_path)
        candidate = (self.root / Path(*parts)).resolve()
        root = self.root.resolve()
        if not candidate.is_relative_to(root) or not candidate.is_file():
            raise StylePackResourceError(
                f"style '{self.reference.name}' resource is unavailable: "
                f"{relative_path}"
            )
        return candidate

    def directory(self, relative_path: str) -> Path:
        """Return an existing directory contained by this style root.

        Args:
            relative_path: POSIX-style path relative to the style root.

        Returns:
            Absolute path to the directory.

        Raises:
            StylePackResourceError: if the path is invalid or unavailable.
        """
        parts = _resource_parts(relative_path)
        candidate = (self.root / Path(*parts)).resolve()
        root = self.root.resolve()
        if not candidate.is_relative_to(root) or not candidate.is_dir():
            raise StylePackResourceError(
                f"style '{self.reference.name}' directory is unavailable: "
                f"{relative_path}"
            )
        return candidate


def _resource_parts(relative_path: str) -> tuple[str, ...]:
    """Validate a manifest resource path and return its components."""
    if not isinstance(relative_path, str) or not relative_path:
        raise StylePackResourceError("style resource path must not be empty")
    if relative_path.startswith(("/", "\\")) or "\\" in relative_path:
        raise StylePackResourceError(
            f"style resource path is not relative: {relative_path}"
        )
    parts = tuple(relative_path.split("/"))
    if any(part in {"", ".", ".."} for part in parts):
        raise StylePackResourceError(
            f"style resource path is invalid: {relative_path}"
        )
    return parts
