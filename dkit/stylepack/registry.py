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
"""Name-based resolution of registered style packs."""

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .config import StyleConfigRepository, StyleConfigError
from .errors import StylePackError
from .loader import StylePackLoader
from .model import StylePack, StylePackReference


class StyleRegistry:
    """Resolve built-in and INI-registered style packs by name."""

    def __init__(
        self,
        config_path: str | Path = "~/.dk.ini",
        builtins: Mapping[str, StylePack] | None = None,
        loader: StylePackLoader | None = None,
    ):
        self.repository = StyleConfigRepository(config_path)
        self.builtins = {name.lower(): pack for name, pack in (builtins or {}).items()}
        self.loader = loader or StylePackLoader()

    def names(self) -> list[str]:
        """Return all available style names in sorted order."""
        names = set(self.builtins) | set(self.repository.references())
        return sorted(names)

    def reference(self, name: str) -> StylePackReference:
        """Return the registered reference for a non-built-in style."""
        normalised = name.lower()
        if normalised in self.builtins:
            raise StylePackError(f"style '{normalised}' is built in")
        try:
            return self.repository.get(normalised)
        except StyleConfigError as exc:
            raise StylePackError(str(exc)) from exc

    def get(self, name: str) -> StylePack:
        """Load a style by its registered name."""
        normalised = name.lower()
        if normalised in self.builtins:
            return self.builtins[normalised]
        return self.loader.load(self.reference(normalised))

    def refresh(self, name: str) -> StylePackReference:
        """Refresh the fingerprint for an installed style registration.

        Args:
            name: registered style name to refresh.

        Returns:
            The updated registration, including its current fingerprint and
            distribution version.
        """
        reference = self.reference(name)
        unverified = reference.model_copy(update={"fingerprint": None})
        pack = self.loader.load(unverified)
        refreshed = unverified.model_copy(update={
            "fingerprint": fingerprint_for(pack),
            "distribution_version": getattr(
                pack.distribution, "version", reference.distribution_version
            ),
        })
        try:
            self.repository.set(refreshed, replace_existing=True)
        except StyleConfigError as exc:
            raise StylePackError(str(exc)) from exc
        return refreshed

    def register(
        self,
        reference: StylePackReference,
        replace_existing: bool = False,
        distribution: Any | None = None,
    ) -> StylePackReference:
        """Validate and persist one style registration.

        Args:
            reference: name and distribution resource to register.
            replace_existing: allow replacing an existing registration.
            distribution: optional metadata object, primarily for tests.

        Returns:
            The persisted reference, including its verified fingerprint.
        """
        reference = reference.model_copy(
            update={"name": reference.name.lower()}
        )
        if reference.name in self.builtins:
            raise StylePackError(f"style '{reference.name}' is built in")
        pack = self.loader.load(reference, distribution=distribution)
        persisted = reference.model_copy(update={
            "fingerprint": fingerprint_for(pack),
            "distribution_version": getattr(
                pack.distribution, "version", reference.distribution_version
            ),
        })
        try:
            self.repository.set(persisted, replace_existing=replace_existing)
        except StyleConfigError as exc:
            raise StylePackError(str(exc)) from exc
        return persisted

    def unregister(self, name: str, force: bool = False):
        """Remove one registered style from the repository."""
        try:
            self.repository.remove(name, force=force)
        except StyleConfigError as exc:
            raise StylePackError(str(exc)) from exc


def fingerprint_for(pack: StylePack) -> str:
    """Return the current fingerprint for a loaded style pack."""
    from .fingerprint import fingerprint_root
    return fingerprint_root(pack.root)
