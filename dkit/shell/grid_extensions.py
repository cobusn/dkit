# Copyright (c) 2026 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Dkit-specific extensions for the curses grid component."""

import math
import re
from typing import Any

from curses_components.grid import CommandHandler, GridComponent


def register_commands(grid: GridComponent) -> None:
    """Register dkit-specific commands on a grid.

    Args:
        grid: Grid component that will receive the extension commands.

    This function is deliberately the single registration point for dkit
    commands.  It keeps command setup separate from the external grid
    component and gives the subclass below a small, explicit constructor.
    """
    for name, (handler, help_text) in _COMMANDS.items():
        grid.register_command(name, handler, help_text=help_text)


class DkitGridComponent(GridComponent):
    """Grid component with dkit-specific commands registered automatically."""

    def __init__(self, *args: Any, **kwargs: Any):
        """Create a grid and register the dkit command extensions.

        Args:
            *args: Positional arguments accepted by ``GridComponent``.
            **kwargs: Keyword arguments accepted by ``GridComponent``.
        """
        super().__init__(*args, **kwargs)
        register_commands(self)


def _cmd_histogram(grid: GridComponent, args: list[str]) -> None:
    """Display a histogram for the currently selected column.

    Args:
        grid: Grid component containing the selected column and data.
        args: Optional command arguments.  The only argument is the number of
            bins.
    """
    if len(args) > 1:
        grid.show_error("usage: histogram [bins]")
        return

    bins = None
    if args:
        try:
            bins = int(args[0])
        except ValueError:
            grid.show_error("histogram bins must be a positive integer")
            return
        if bins < 1:
            grid.show_error("histogram bins must be a positive integer")
            return

    if not grid.columns or grid.col_idx >= len(grid.columns):
        grid.show_error("histogram requires a selected column")
        return

    column = grid.columns[grid.col_idx]
    values = []
    ignored = 0
    for row in grid.data:
        try:
            value = float(row.get(column))
        except (TypeError, ValueError):
            ignored += 1
            continue
        if not math.isfinite(value):
            ignored += 1
            continue
        values.append(value)

    if not values:
        grid.show_error(f"no numeric values in column '{column}'")
        return

    from dkit.data.histogram import Histogram
    from dkit.shell.console import render_histogram

    automatic_bins = bins is None
    try:
        if automatic_bins:
            bins = _automatic_bin_count(values, grid)
            histogram = Histogram.from_data(
                values,
                bins=bins,
                range_mode="tukey",
            )
        else:
            histogram = Histogram.from_data(values, bins=bins)
        rendered = render_histogram(
            histogram,
            width=_histogram_width(grid),
            line_color=None,
        )
    except ImportError:
        grid.show_error("histogram requires the plotille package")
        return
    except (TypeError, ValueError) as exc:
        grid.show_error(f"could not render histogram: {exc}")
        return

    title = f"histogram: {column}"
    outlier_counts = []
    if automatic_bins and math.isinf(histogram.bins[0].left):
        outlier_counts.append(f"{histogram.bins[0].count} low")
    if automatic_bins and math.isinf(histogram.bins[-1].right):
        outlier_counts.append(f"{histogram.bins[-1].count} high")
    if outlier_counts:
        title += f" ({'; '.join(outlier_counts)} outliers)"
    if ignored:
        title += f" (ignored {ignored} non-numeric values)"
    lines = _strip_ansi(rendered).splitlines()
    width, height = _popup_dimensions(grid, title, lines)
    grid.show_popup(
        title,
        lines=lines,
        key_col_width=_popup_key_width(width),
        width=width,
        height=height,
    )


def _cmd_summary(grid: GridComponent, args: list[str]) -> None:
    """Display an accumulator summary for the currently selected column.

    Args:
        grid: Grid component containing the selected column and data.
        args: Command arguments, which must be empty.
    """
    if args:
        grid.show_error("usage: summary")
        return

    if not grid.columns or grid.col_idx >= len(grid.columns):
        grid.show_error("summary requires a selected column")
        return

    column = grid.columns[grid.col_idx]
    values = []
    ignored = 0
    for row in grid.data:
        try:
            value = float(row.get(column))
        except (TypeError, ValueError):
            ignored += 1
            continue
        if not math.isfinite(value):
            ignored += 1
            continue
        values.append(value)

    if not values:
        grid.show_error(f"no numeric values in column '{column}'")
        return

    from dkit.data.stats import Accumulator

    title = f"summary: {column}"
    if ignored:
        title += f" (ignored {ignored} non-numeric values)"
    lines = str(Accumulator(values)).splitlines()
    width, height = _popup_dimensions(grid, title, lines)
    grid.show_popup(
        title,
        lines=lines,
        key_col_width=_popup_key_width(width),
        width=width,
        height=height,
    )


def _cmd_insert(grid: GridComponent, args: list[str]) -> None:
    """Insert a calculated column at the current position.

    Args:
        grid: Grid component containing the rows to update.
        args: New column name followed by an infix expression.
    """
    if len(args) < 2:
        grid.show_error("usage: insert <name> <expression>")
        return

    column_name = args[0]
    if column_name in grid.columns:
        grid.show_error(f"column already exists: {column_name}")
        return

    expression = " ".join(args[1:])
    try:
        from dkit.parsers.infix_parser import ExpressionParser

        parser = ExpressionParser(expression)
    except Exception as exc:
        grid.show_error(f"invalid expression: {exc}")
        return

    values = []
    try:
        for row_number, row in enumerate(grid._all_data, start=1):
            values.append(parser(row))
    except Exception as exc:
        grid.show_error(f"insert failed at row {row_number}: {exc}")
        return

    for row, value in zip(grid._all_data, values):
        row[column_name] = value
    grid.columns.insert(grid.col_idx, column_name)
    grid.col_widths[column_name] = min(
        grid.max_col_width,
        max(
            len(column_name),
            *(
                len(_grid_display_value(grid, row.get(column_name, "")))
                for row in grid._all_data
            ),
        ),
    )
    grid.col_is_numeric[column_name] = any(
        grid.is_number(str(row.get(column_name, "")))
        for row in grid._all_data
    )


def _cmd_list_functions(grid: GridComponent, args: list[str]) -> None:
    """Display functions available to expression-based grid commands.

    Args:
        grid: Grid component used to display the function list.
        args: Command arguments, which must be empty.
    """
    if args:
        grid.show_error("usage: list-functions")
        return

    from dkit.parsers.infix_parser import ExpressionParser

    parser = ExpressionParser()
    lines = [f"{name}(x)" for name in parser._f1_map]
    lines.extend(f"{name}(x1, x2)" for name in parser._f2_map)
    lines.sort()
    title = "expression functions"
    width, height = _popup_dimensions(grid, title, lines)
    grid.show_popup(
        title,
        lines=lines,
        key_col_width=_popup_key_width(width),
        width=width,
        height=height,
    )


def _grid_display_value(grid: GridComponent, value: object) -> str:
    """Format a value as it will appear in the grid.

    Args:
        grid: Grid component that owns the display format.
        value: Cell value to format.

    Returns:
        Value formatted consistently with ``GridComponent._draw_cell()``.
    """
    if isinstance(value, float):
        return format(value, grid.float_fmt)
    return str(value)


def _histogram_width(grid: GridComponent) -> int | None:
    """Return a histogram width that fits inside the grid popup.

    Args:
        grid: Grid component whose curses screen determines the popup size.

    Returns:
        Plotille chart width, or ``None`` when the screen is not initialized.
    """
    if grid.stdscr is None:
        return None

    screen_width = grid.stdscr.getmaxyx()[1]
    popup_width = min(80, screen_width - 2)
    popup_content_width = max(1, popup_width - 5)
    # Plotille's histogram includes labels and axis decorations in addition
    # to the requested plot width.
    return max(1, popup_content_width - 36)


def _automatic_bin_count(values: list[float], grid: GridComponent) -> int:
    """Choose a data-driven bin count that fits the histogram popup.

    Args:
        values: Numeric values selected from the grid column.
        grid: Grid component whose screen constrains popup height.

    Returns:
        Freedman-Diaconis bin count capped to the available popup rows.
    """
    import numpy

    edges = numpy.histogram_bin_edges(values, bins="fd")
    estimated_bins = len(edges) - 1
    if grid.stdscr is None:
        return min(estimated_bins, 18)

    screen_height = grid.stdscr.getmaxyx()[0]
    popup_height = min(25, screen_height - 2)
    # The Plotille result has a header and footer; popup chrome consumes four
    # more rows.  Keep all bucket rows visible without scrolling.
    visible_bins = max(1, popup_height - 8)
    return min(estimated_bins, visible_bins)


def _popup_dimensions(
    grid: GridComponent,
    title: str,
    lines: list[str],
) -> tuple[int | None, int | None]:
    """Calculate popup dimensions from rendered text and screen size.

    Args:
        grid: Grid component whose curses screen determines available space.
        title: Popup title.
        lines: Popup content lines.

    Returns:
        Tuple containing the popup width and height, or ``None`` dimensions
        when curses has not initialized a screen.
    """
    if grid.stdscr is None:
        return None, None

    screen_height, screen_width = grid.stdscr.getmaxyx()
    content_width = max(
        [len(title), *(len(line) for line in lines)],
        default=0,
    )
    width = min(80, screen_width - 2, content_width + 4)
    height = min(25, screen_height - 2, len(lines) + 4)
    return max(1, width), max(1, height)


def _popup_key_width(width: int | None) -> int:
    """Return a text-popup key width that preserves complete lines.

    Args:
        width: Calculated outer popup width, if a screen is available.

    Returns:
        Key-column width for the popup content.
    """
    return width - 2 if width is not None else 76


def _strip_ansi(value: str) -> str:
    """Remove terminal control sequences before text enters curses.

    Args:
        value: Rendered terminal text.

    Returns:
        Text without ANSI control sequences.
    """
    return re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", value)


_COMMANDS: dict[str, tuple[CommandHandler, str]] = {
    "histogram": (
        _cmd_histogram,
        "histogram [bins]  Show a histogram for the current numeric column",
    ),
    "insert": (
        _cmd_insert,
        "insert <name> <expression>  Insert a calculated column at the "
        "current position",
    ),
    "list-functions": (
        _cmd_list_functions,
        "list-functions  List expression functions available to insert",
    ),
    "summary": (
        _cmd_summary,
        "summary           Show numeric summary statistics for the current "
        "column",
    ),
}
