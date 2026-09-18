import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "..")  # noqa

from dkit.shell import terminal_image


class TestShowHistogram(unittest.TestCase):

    @patch("dkit.shell.terminal_image.show")
    @patch("dkit.plot2.plot_histogram")
    def test_defaults_to_dark_theme(self, plot_histogram, show):
        figure = object()
        plot_histogram.return_value = figure

        terminal_image.show_histogram("histogram")

        plot_histogram.assert_called_once_with(
            "histogram", theme="dkit-dark"
        )
        show.assert_called_once_with(figure, dpi=150)

    @patch("dkit.shell.terminal_image.show")
    @patch("dkit.plot2.plot_histogram")
    def test_preserves_explicit_theme(self, plot_histogram, show):
        figure = object()
        plot_histogram.return_value = figure

        terminal_image.show_histogram("histogram", theme="dkit-light")

        plot_histogram.assert_called_once_with(
            "histogram", theme="dkit-light"
        )
        show.assert_called_once_with(figure, dpi=150)


if __name__ == "__main__":
    unittest.main()
