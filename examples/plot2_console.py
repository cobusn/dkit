"""Display a histogram and treemap using a selectable Plot2 theme.

Usage::

    python examples/plot2_console.py
    python examples/plot2_console.py --theme dkit-dark

The terminal image display requires the system ``chafa`` command.
"""
import argparse

from dkit.data.histogram import Histogram
from dkit.plot2 import quick
from dkit.plot2.theme import themes
from dkit.shell import terminal_image


def parse_args():
    """Parse example command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--theme",
        choices=sorted(themes),
        default="dkit-console",
        help="plot2 theme (default: dkit-console)",
    )
    return parser.parse_args()


def main():
    """Generate and display the example plots."""
    args = parse_args()
    histogram = Histogram.from_data(
        [2, 3, 3, 4, 4, 4, 5, 5, 6, 7, 8, 9],
        bins=6,
    )
    terminal_image.show_histogram(
        histogram,
        theme=args.theme,
        title="example distribution",
        xlabel="value",
    )

    treemap_data = [
        {"category": "green", "value": 40},
        {"category": "amber", "value": 25},
        {"category": "red", "value": 15},
        {"category": "cyan", "value": 12},
        {"category": "gray", "value": 8},
    ]
    figure = quick.treemap(
        treemap_data,
        label_field="category",
        value_field="value",
        title="example allocation",
        theme=args.theme,
    )
    terminal_image.show(figure)


if __name__ == "__main__":
    main()
