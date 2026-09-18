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
"""Deterministic fingerprints for durable style-pack resources."""

from hashlib import sha256
from pathlib import Path

from .errors import StylePackResourceError


def fingerprint_root(root: Path) -> str:
    """Return a deterministic SHA-256 fingerprint for a style root.

    Transient Python bytecode below ``__pycache__`` is excluded.  Pip may
    create it while installing a style distribution, but it is not a style
    resource and must not invalidate a registration.

    Args:
        root: directory containing one style manifest and its resources.

    Returns:
        Digest prefixed with ``sha256:``.

    Raises:
        StylePackResourceError: if the root is invalid or contains an escaping
            symlink.
    """
    root = root.resolve()
    if not root.is_dir():
        raise StylePackResourceError(f"style resource root is unavailable: {root}")

    digest = sha256()
    for path in sorted(root.rglob("*"), key=lambda value: value.as_posix()):
        relative_path = path.relative_to(root)
        if "__pycache__" in relative_path.parts:
            continue
        if not path.is_file():
            continue
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            raise StylePackResourceError(
                f"style resource escapes its root: {relative_path}"
            )
        relative = relative_path.as_posix().encode("utf-8")
        digest.update(relative)
        digest.update(b"\0")
        with path.open("rb") as infile:
            for block in iter(lambda: infile.read(1024 * 1024), b""):
                digest.update(block)
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"
