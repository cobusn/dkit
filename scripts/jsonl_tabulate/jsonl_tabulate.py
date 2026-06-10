#!/usr/bin/env python
"""Tabulate JSONL from stdin or files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

__version__ = "26.6.1"


def _iter_lines(files: list[str]) -> Iterable[str]:
    """Yield lines from files or stdin."""
    if not files or files == ["-"]:
        yield from sys.stdin
        return

    for name in files:
        if name == "-":
            yield from sys.stdin
            continue

        with Path(name).open("rt", encoding="utf-8") as infile:
            yield from infile


def _load_jsonl(lines: Iterable[str]) -> list[dict]:
    """Parse JSONL records into dictionaries."""
    rows: list[dict] = []
    for line_no, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line:
            continue

        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Invalid JSON on line {line_no}: {exc}") from exc

        if isinstance(item, dict):
            rows.append(item)
        else:
            rows.append({"value": item})

    return rows


def _collect_headers(rows: list[dict], sort_keys: bool = False) -> list[str]:
    """Collect column names from the input rows."""
    headers: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)

    if sort_keys:
        headers.sort()

    return headers


def _rows_to_table(rows: list[dict], headers: list[str]) -> list[list[object]]:
    """Convert dict rows to a 2D table."""
    return [[row.get(header, "") for header in headers] for row in rows]


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="jsonl-tabulate",
        description="Tabulate JSONL from stdin or files.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        default=["-"],
        help="JSONL files to read, or '-' / no argument for stdin.",
    )
    parser.add_argument(
        "-f",
        "--format",
        default="simple",
        help="tabulate table format (default: simple)",
    )
    parser.add_argument(
        "--sort-keys",
        action="store_true",
        help="sort columns alphabetically instead of preserving first-seen order",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the jsonl-tabulate CLI."""
    from tabulate import tabulate

    parser = build_parser()
    args = parser.parse_args(argv)

    rows = _load_jsonl(_iter_lines(args.files))
    if not rows:
        return 0

    headers = _collect_headers(rows, sort_keys=args.sort_keys)
    table = _rows_to_table(rows, headers)
    print(tabulate(table, headers=headers, tablefmt=args.format))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
