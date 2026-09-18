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
"""Safe loading of declarative style packs from installed distributions."""

from importlib.metadata import Distribution
from json import loads
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import url2pathname

import yaml
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version
from pydantic import ValidationError

from .. import __version__
from .fingerprint import fingerprint_root
from .errors import (
    StylePackCompatibilityError,
    StylePackFingerprintError,
    StylePackManifestError,
    StylePackResourceError,
)
from .model import StyleManifest, StylePack, StylePackReference, _resource_parts


DistributionFactory = Callable[[str], Any]


class StylePackLoader:
    """Load and validate a registered declarative style pack."""

    def __init__(self, distribution_factory: DistributionFactory | None = None):
        self.distribution_factory = distribution_factory or Distribution.from_name

    def load(
        self,
        reference: StylePackReference,
        distribution: Any | None = None,
    ) -> StylePack:
        """Load one registered style pack.

        Args:
            reference: registered distribution and manifest information.
            distribution: optional distribution metadata, primarily for tests.

        Returns:
            A validated immutable style pack.

        Raises:
            StylePackManifestError: if the manifest cannot be validated.
            StylePackResourceError: if a resource is unsafe or missing.
            StylePackCompatibilityError: if the dkit requirement is unmet.
            StylePackFingerprintError: if the registered digest is stale.
        """
        distribution = distribution or self._find_distribution(reference)
        distribution_root, manifest_path = self._manifest_path(
            distribution, reference.manifest
        )
        if not manifest_path.is_file():
            raise StylePackManifestError(
                f"style '{reference.name}' manifest is unavailable: "
                f"{reference.manifest}"
            )

        manifest = self._read_manifest(manifest_path, reference.name)
        if manifest.id != reference.name:
            raise StylePackManifestError(
                f"style '{reference.name}' manifest has id '{manifest.id}'"
            )
        self._check_compatibility(manifest, reference.name)

        style_root = manifest_path.parent.resolve()
        self._validate_resources(manifest, style_root, reference.name)
        actual_fingerprint = fingerprint_root(style_root)
        if reference.fingerprint and reference.fingerprint != actual_fingerprint:
            raise StylePackFingerprintError(
                f"style '{reference.name}' fingerprint does not match its "
                "registration"
            )
        return StylePack(reference, manifest, distribution, style_root)

    def _find_distribution(self, reference: StylePackReference) -> Any:
        """Find the installed distribution named by a registration."""
        try:
            return self.distribution_factory(reference.distribution)
        except Exception as exc:
            raise StylePackResourceError(
                f"style distribution '{reference.distribution}' is not installed"
            ) from exc

    @classmethod
    def _manifest_path(
        cls,
        distribution: Any,
        manifest: str,
    ) -> tuple[Path, Path]:
        """Resolve a manifest from installed or editable distribution files.

        PEP 660 installations can map imports to a source checkout while
        ``Distribution.locate_file`` still points to ``site-packages``.  The
        fallback uses only the distribution's PEP 610 metadata and never
        imports the style provider.

        Args:
            distribution: distribution metadata for the registered pack.
            manifest: POSIX-style manifest path relative to its package root.

        Returns:
            The root containing the resolved manifest and the manifest path.
        """
        parts = _resource_parts(manifest)
        distribution_root = cls._path_from_distribution(distribution, "")
        manifest_path = cls._path_from_distribution(distribution, manifest)
        cls._require_inside(manifest_path, distribution_root, manifest)
        if manifest_path.is_file():
            return distribution_root, manifest_path

        for source_root in cls._editable_roots(distribution):
            for package_root in (source_root, source_root / "src"):
                if not package_root.is_dir():
                    continue
                candidate = (package_root / Path(*parts)).resolve()
                cls._require_inside(candidate, package_root, manifest)
                if candidate.is_file():
                    return package_root, candidate
        return distribution_root, manifest_path

    @staticmethod
    def _editable_roots(distribution: Any) -> tuple[Path, ...]:
        """Return local source roots declared by editable PEP 610 metadata.

        Args:
            distribution: distribution metadata for the registered pack.

        Returns:
            Existing local source roots, or an empty tuple when unavailable.
        """
        try:
            source = distribution.read_text("direct_url.json")
            values = loads(source) if source else {}
            directory_info = values.get("dir_info", {})
            url = values.get("url")
        except (
            AttributeError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            return ()
        if not isinstance(directory_info, dict) or not directory_info.get(
            "editable"
        ):
            return ()
        if not isinstance(url, str):
            return ()

        parsed = urlparse(url)
        if (
            parsed.scheme != "file"
            or not parsed.path
            or parsed.netloc not in {"", "localhost"}
        ):
            return ()
        try:
            root = Path(url2pathname(parsed.path)).resolve()
        except (OSError, RuntimeError, ValueError):
            return ()
        return (root,) if root.is_dir() else ()

    @staticmethod
    def _path_from_distribution(distribution: Any, relative_path: str) -> Path:
        """Resolve a distribution resource without importing its package."""
        try:
            return Path(distribution.locate_file(relative_path))
        except Exception as exc:
            raise StylePackResourceError(
                f"cannot locate style distribution resource: {relative_path}"
            ) from exc

    @staticmethod
    def _require_inside(path: Path, root: Path, label: str):
        """Require a path to remain inside a distribution or style root."""
        try:
            path.resolve().relative_to(root.resolve())
        except ValueError as exc:
            raise StylePackResourceError(
                f"style resource escapes its distribution: {label}"
            ) from exc

    @staticmethod
    def _read_manifest(path: Path, name: str) -> StyleManifest:
        """Parse one YAML manifest into its typed model."""
        try:
            with path.open("rt", encoding="utf-8") as infile:
                values = yaml.safe_load(infile)
            return StyleManifest.model_validate(values)
        except (OSError, yaml.YAMLError, ValidationError, TypeError) as exc:
            raise StylePackManifestError(
                f"style '{name}' has an invalid manifest"
            ) from exc

    @staticmethod
    def _check_compatibility(manifest: StyleManifest, name: str):
        """Check manifest dkit version requirements."""
        try:
            requirement = SpecifierSet(manifest.requires_dkit)
            current = Version(__version__)
        except (InvalidSpecifier, InvalidVersion) as exc:
            raise StylePackManifestError(
                f"style '{name}' has an invalid dkit version requirement"
            ) from exc
        if current not in requirement:
            raise StylePackCompatibilityError(
                f"style '{name}' requires dkit {manifest.requires_dkit}; "
                f"installed version is {__version__}"
            )

    @classmethod
    def _validate_resources(
        cls,
        manifest: StyleManifest,
        root: Path,
        name: str,
    ):
        """Validate every resource reference declared by the manifest."""
        references: list[tuple[str, bool]] = []
        if manifest.formats.reportlab:
            references.extend([
                (manifest.formats.reportlab.layout, False),
                (manifest.formats.reportlab.cover, False),
            ])
        if manifest.formats.html:
            references.extend([
                (manifest.formats.html.stylesheet, False),
                (manifest.formats.html.email_stylesheet, False),
            ])
        if manifest.formats.latex:
            references.append((manifest.formats.latex.resources, True))
        if manifest.formats.docx:
            references.append((manifest.formats.docx.template, False))
        if manifest.formats.matplotlib:
            references.extend([
                (manifest.formats.matplotlib.screen, False),
                (manifest.formats.matplotlib.print, False),
            ])
        for font in manifest.fonts:
            references.extend(
                (path, False)
                for path in (
                    font.regular,
                    font.bold,
                    font.italic,
                    font.bold_italic,
                )
                if path is not None
            )

        for relative_path, directory in references:
            parts = _resource_parts(relative_path)
            candidate = (root / Path(*parts)).resolve()
            cls._require_inside(candidate, root, relative_path)
            valid = candidate.is_dir() if directory else candidate.is_file()
            if not valid:
                kind = "directory" if directory else "file"
                raise StylePackResourceError(
                    f"style '{name}' {kind} is unavailable: {relative_path}"
                )
