import unittest
from unittest.mock import patch

from dkit.shell.grid_extensions import (
    DkitGridComponent,
    _cmd_histogram,
    _cmd_insert,
    _cmd_list_functions,
    _cmd_summary,
)


class FakeGrid:
    """Minimal grid surface required by the histogram command."""

    def __init__(self, values):
        self.columns = ["value"]
        self.col_idx = 0
        self.data = [{"value": value} for value in values]
        self._all_data = self.data
        self.max_col_width = 20
        self.float_fmt = ".2f"
        self.col_widths = {"value": 5}
        self.col_is_numeric = {"value": True}
        self.errors = []
        self.popups = []
        self.stdscr = None

    def show_error(self, message):
        self.errors.append(message)

    def show_popup(
        self,
        title,
        *,
        lines,
        key_col_width=20,
        width=None,
        height=None,
    ):
        self.popups.append((title, lines, key_col_width, width, height))

    @staticmethod
    def is_number(value):
        try:
            float(value)
        except (TypeError, ValueError):
            return False
        return True


class TestGridExtensions(unittest.TestCase):

    def test_histogram_command_is_registered(self):
        grid = DkitGridComponent()
        self.assertIn("histogram", grid.commands)
        self.assertIn("insert", grid.commands)
        self.assertIn("list-functions", grid.commands)
        self.assertIn("summary", grid.commands)
        self.assertEqual(
            grid._extension_help,
            {
                "histogram": (
                    "histogram [bins]  Show a histogram for the current "
                    "numeric column"
                ),
                "insert": (
                    "insert <name> <expression>  Insert a calculated column "
                    "at the current position"
                ),
                "list-functions": (
                    "list-functions  List expression functions available to "
                    "insert"
                ),
                "summary": (
                    "summary           Show numeric summary statistics for "
                    "the current column"
                ),
            },
        )

    @patch("dkit.shell.console.render_histogram", return_value="plot\nline")
    def test_histogram_uses_auto_bins_and_ignores_non_numeric_values(
        self, render_histogram
    ):
        grid = FakeGrid([1, "bad", 2, None, float("nan")])

        _cmd_histogram(grid, [])

        render_histogram.assert_called_once()
        self.assertEqual(
            grid.popups,
            [
                (
                    "histogram: value (ignored 3 non-numeric values)",
                    ["plot", "line"],
                    76,
                    None,
                    None,
                ),
            ],
        )
        self.assertEqual(grid.errors, [])

    @patch("dkit.shell.console.render_histogram", return_value="plot")
    def test_histogram_limits_plot_width_to_popup(self, render_histogram):
        grid = FakeGrid([1, 2, 3])
        grid.stdscr = type("Screen", (), {"getmaxyx": lambda self: (40, 120)})()

        _cmd_histogram(grid, [])

        render_histogram.assert_called_once_with(
            render_histogram.call_args.args[0],
            width=39,
            line_color=None,
        )
        self.assertEqual(grid.popups[0][2:], (18, 20, 5))

    @patch("dkit.shell.console.render_histogram", return_value="plot")
    def test_histogram_passes_explicit_bin_count(self, render_histogram):
        grid = FakeGrid([1, 2, 3])

        with patch("dkit.data.histogram.Histogram.from_data") as from_data:
            from_data.return_value.bins = [object()]
            _cmd_histogram(grid, ["4"])

        from_data.assert_called_once_with([1.0, 2.0, 3.0], bins=4)
        render_histogram.assert_called_once_with(
            from_data.return_value,
            width=None,
            line_color=None,
        )

    def test_histogram_rejects_invalid_bin_count(self):
        grid = FakeGrid([1, 2, 3])

        _cmd_histogram(grid, ["0"])

        self.assertEqual(
            grid.errors,
            ["histogram bins must be a positive integer"],
        )
        self.assertEqual(grid.popups, [])

    def test_histogram_rejects_column_without_numeric_values(self):
        grid = FakeGrid(["one", "two"])

        _cmd_histogram(grid, [])

        self.assertEqual(
            grid.errors,
            ["no numeric values in column 'value'"],
        )
        self.assertEqual(grid.popups, [])

    def test_summary_displays_numeric_values_and_ignored_count(self):
        grid = FakeGrid([1, "bad", 3])

        _cmd_summary(grid, [])

        self.assertEqual(
            grid.popups[0][0],
            "summary: value (ignored 1 non-numeric values)",
        )
        self.assertIn("Observations:       2", grid.popups[0][1])
        self.assertIn("Mean:               2.000000", grid.popups[0][1])
        self.assertEqual(grid.errors, [])

    def test_summary_rejects_column_without_numeric_values(self):
        grid = FakeGrid(["one", "two"])

        _cmd_summary(grid, [])

        self.assertEqual(
            grid.errors,
            ["no numeric values in column 'value'"],
        )
        self.assertEqual(grid.popups, [])

    def test_summary_rejects_arguments(self):
        grid = FakeGrid([1, 2])

        _cmd_summary(grid, ["extra"])

        self.assertEqual(grid.errors, ["usage: summary"])
        self.assertEqual(grid.popups, [])

    def test_insert_evaluates_expression_for_all_rows(self):
        grid = FakeGrid([])
        grid.columns = ["UnitPrice", "Quantity"]
        grid.data = [
            {"UnitPrice": 2, "Quantity": 3},
            {"UnitPrice": 4, "Quantity": 5},
        ]
        grid._all_data = grid.data
        grid.col_idx = 1

        _cmd_insert(grid, ["total", "${UnitPrice}", "*", "${Quantity}"])

        self.assertEqual(grid.columns, ["UnitPrice", "total", "Quantity"])
        self.assertEqual([row["total"] for row in grid.data], [6, 20])
        self.assertEqual(grid.col_widths["total"], 5)
        self.assertTrue(grid.col_is_numeric["total"])
        self.assertEqual(grid.errors, [])

    def test_insert_does_not_change_rows_when_expression_fails(self):
        grid = FakeGrid([1, 2])
        original_columns = list(grid.columns)
        original_rows = [dict(row) for row in grid.data]

        _cmd_insert(grid, ["result", "1", "/", "0"])

        self.assertEqual(grid.columns, original_columns)
        self.assertEqual(grid.data, original_rows)
        self.assertTrue(grid.errors[0].startswith("insert failed at row 1:"))

    def test_insert_rejects_duplicate_column_name(self):
        grid = FakeGrid([1, 2])

        _cmd_insert(grid, ["value", "1"])

        self.assertEqual(grid.errors, ["column already exists: value"])

    def test_insert_sizes_float_column_from_display_format(self):
        grid = FakeGrid([1, 2])

        _cmd_insert(grid, ["ratio", "1", "/", "3"])

        self.assertEqual(grid.col_widths["ratio"], len("ratio"))

    def test_list_functions_displays_sorted_parser_functions(self):
        grid = FakeGrid([])

        _cmd_list_functions(grid, [])

        self.assertEqual(grid.popups[0][0], "expression functions")
        self.assertIn("abs(x)", grid.popups[0][1])
        self.assertIn("replace_na(x1, x2)", grid.popups[0][1])
        self.assertEqual(grid.popups[0][1], sorted(grid.popups[0][1]))
        self.assertEqual(grid.errors, [])

    def test_list_functions_rejects_arguments(self):
        grid = FakeGrid([])

        _cmd_list_functions(grid, ["extra"])

        self.assertEqual(grid.errors, ["usage: list-functions"])
        self.assertEqual(grid.popups, [])

    @patch(
        "dkit.shell.console.render_histogram",
        return_value="\x1b[92mplot\x1b[0m",
    )
    def test_histogram_strips_ansi_sequences_for_popup(self, _render_histogram):
        grid = FakeGrid([1, 2, 3])

        _cmd_histogram(grid, [])

        self.assertEqual(grid.popups[0][1], ["plot"])


if __name__ == "__main__":
    unittest.main()
