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
"""Persistent INI storage for registered style-pack references."""

from configparser import ConfigParser, Error as ConfigParserError
from os import replace, stat
from pathlib import Path
import re
import tempfile

from .errors import StylePackError
from .model import StylePackReference


class StyleConfigError(StylePackError):
    """The style registry configuration is invalid."""


class StyleConfigRepository:
    """Read and update style registrations in one INI file."""

    SECTION_PREFIX = "style:"
    _SECTION_PATTERN = re.compile(r"(?m)^\s*\[[^\]\r\n]+\]\s*\r?\n?")
    _REFERENCE_OPTIONS = {
        "distribution",
        "distribution_version",
        "manifest",
        "fingerprint",
    }

    def __init__(self, path: str | Path = "~/.dk.ini"):
        self.path = Path(path).expanduser()

    def read(self) -> ConfigParser:
        """Read the selected INI file, allowing it not to exist."""
        parser = ConfigParser()
        if self.path.exists():
            try:
                parser.read(self.path, encoding="utf-8")
            except ConfigParserError as exc:
                raise StyleConfigError(
                    f"configuration file is invalid: {self.path}"
                ) from exc
        return parser

    def references(self) -> dict[str, StylePackReference]:
        """Return all registered style references keyed by normalised name."""
        parser = self.read()
        result = {}
        for section in parser.sections():
            if not section.lower().startswith(self.SECTION_PREFIX):
                continue
            name = section[len(self.SECTION_PREFIX):].lower()
            if not name:
                raise StyleConfigError("style registration name is empty")
            values = dict(parser._sections[section])
            values.pop("__name__", None)
            unknown = set(values) - self._REFERENCE_OPTIONS
            if unknown:
                options = ", ".join(sorted(unknown))
                raise StyleConfigError(
                    f"style '{name}' has unknown options: {options}"
                )
            values["name"] = name
            try:
                result[name] = StylePackReference.model_validate(values)
            except Exception as exc:
                raise StyleConfigError(
                    f"style '{name}' has an invalid registration"
                ) from exc
        return result

    def get(self, name: str) -> StylePackReference:
        """Return one registered reference by case-insensitive name."""
        normalised = name.lower()
        try:
            return self.references()[normalised]
        except KeyError as exc:
            raise StyleConfigError(f"unknown style '{normalised}'") from exc

    def set(self, reference: StylePackReference, replace_existing: bool = False):
        """Create or replace one style registration atomically."""
        name = reference.name.lower()
        existing = self.references()
        if name in existing and not replace_existing:
            raise StyleConfigError(f"style '{name}' is already registered")
        section = self._section_text(reference)
        original = self.path.read_text(encoding="utf-8") if self.path.exists() else ""
        updated = self._replace_section(original, name, section)
        self._write(updated)

    def remove(self, name: str, force: bool = False):
        """Remove one registration without removing the distribution."""
        normalised = name.lower()
        self.get(normalised)
        parser = self.read()
        configured_default = parser.get("DOC", "style", fallback="").lower()
        if configured_default == normalised and not force:
            raise StyleConfigError(
                f"style '{normalised}' is the configured DOC default; "
                "change it first or pass --force"
            )
        original = self.path.read_text(encoding="utf-8")
        updated = self._replace_section(original, normalised, "")
        self._write(updated)

    @classmethod
    def _section_text(cls, reference: StylePackReference) -> str:
        """Render one canonical registration section."""
        lines = [f"[{cls.SECTION_PREFIX}{reference.name}]\n"]
        values = reference.model_dump(exclude={"name"}, exclude_none=True)
        for key in (
            "distribution",
            "distribution_version",
            "manifest",
            "fingerprint",
        ):
            if key in values:
                lines.append(f"{key} = {values[key]}\n")
        return "".join(lines)

    @classmethod
    def _replace_section(cls, content: str, name: str, replacement: str) -> str:
        """Replace one named section while preserving all other bytes."""
        target = f"[{cls.SECTION_PREFIX}{name}]".lower()
        matches = list(cls._SECTION_PATTERN.finditer(content))
        for index, match in enumerate(matches):
            header = match.group(0).strip().lower()
            if header != target:
                continue
            end = (
                matches[index + 1].start()
                if index + 1 < len(matches)
                else len(content)
            )
            before = content[:match.start()]
            after = content[end:]
            if replacement and before and not before.endswith("\n"):
                before += "\n"
            if replacement and after and not replacement.endswith("\n"):
                replacement += "\n"
            return before + replacement + after

        if not replacement:
            raise StyleConfigError(f"unknown style '{name}'")
        if content and not content.endswith("\n"):
            content += "\n"
        return content + ("\n" if content else "") + replacement

    def _write(self, content: str):
        """Validate and atomically write updated INI content."""
        parser = ConfigParser()
        try:
            parser.read_string(content)
        except ConfigParserError as exc:
            raise StyleConfigError(
                f"updated configuration is invalid: {self.path}"
            ) from exc

        self.path.parent.mkdir(parents=True, exist_ok=True)
        mode = 0o600
        if self.path.exists():
            mode = stat(self.path).st_mode & 0o777
        handle = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=self.path.parent,
            prefix=f".{self.path.name}.", delete=False,
        )
        temporary = Path(handle.name)
        try:
            temporary.chmod(mode)
            handle.write(content)
            handle.flush()
            handle.close()
            replace(temporary, self.path)
        except Exception:
            handle.close()
            temporary.unlink(missing_ok=True)
            raise
