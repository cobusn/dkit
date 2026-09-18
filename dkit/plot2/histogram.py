"""Matplotlib rendering helpers for precomputed histograms."""

from typing import Union

from matplotlib.axes import Axes
from matplotlib.figure import Figure

from ..data.histogram import Histogram
from .theme import Theme


def plot_histogram(
    histogram: Histogram,
    label: Union[str, None] = None,
    title: Union[str, None] = None,
    xlabel: Union[str, None] = None,
    ylabel: str = "Frequency",
    color: Union[str, None] = None,
    xscale=None,
    yscale=None,
    theme: Union[Theme, str, None] = None,
    ax: Union[Axes, None] = None,
    **options,
) -> Figure:
    """Render an existing Histogram as a Plot2 bar chart.

    Args:
        histogram: Precomputed histogram to render.
        label: Optional bar legend label.
        title: Optional chart title.
        xlabel: Optional x-axis label.
        ylabel: Y-axis label.
        color: Bar colour.
        xscale: Explicit x-axis scale.
        yscale: Explicit y-axis scale.
        theme: Plot2 theme.
        ax: Existing Matplotlib axes, if any.
        **options: Additional options passed to ``quick.bar``.

    Returns:
        The rendered Matplotlib figure.
    """
    from . import quick

    rows = []
    for row in histogram.plot_data():
        rows.append({
            key: float(value) if key != "count" else value
            for key, value in row.items()
        })

    return quick.bar(
        rows,
        x="midpoint",
        y="count",
        label=label,
        width="width",
        title=title,
        xlabel=xlabel,
        ylabel=ylabel,
        color=color,
        xscale=xscale,
        yscale=yscale,
        theme=theme,
        ax=ax,
        **options,
    )
