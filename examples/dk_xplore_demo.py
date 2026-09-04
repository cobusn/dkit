"""
dk_xplore_demo.py
==================
Generates the sample images used in doc/source/howto_cli_exploration.rst.

Each call here is the same command shown in that guide, run with -o instead
of leaving it to draw in the terminal, so the documentation can show what
the terminal output looks like without trying to reproduce a chafa render as
static text. See :mod:`dkit.plot2` for the themes and chart types themselves.

Output is written to plots/dk_xplore_*.png.
"""
import sys; sys.path.insert(0, "..")  # noqa

from lib_dk.explore_module import ExploreModule

DATA = "data/mpg.jsonl"

ExploreModule([
    "histogram", "-d", "displ", "-o", "plots/dk_xplore_histogram.png", DATA,
]).run()

ExploreModule([
    "plot", "-x", "cty", "-y", "hwy", "--type", "scatter",
    "-o", "plots/dk_xplore_plot_scatter.png", DATA,
]).run()

ExploreModule([
    "plot", "-x", "cty", "-y", "hwy", "--theme", "dkit-light",
    "-o", "plots/dk_xplore_plot_light.png", DATA,
]).run()

print("wrote plots/dk_xplore_histogram.png")
print("wrote plots/dk_xplore_plot_scatter.png")
print("wrote plots/dk_xplore_plot_light.png")
