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
"""
Axis scales for dkit.plot2

A scale is a small immutable value that owns everything about one axis: its
label, its limits, its locator and formatter, tick rotation, and whether ticks
are drawn at all.  It also *encodes* raw field values into the coordinates
matplotlib plots in, which is the difference between :class:`Categorical` (a
label per position) and :class:`Linear`, :class:`Log` and :class:`Time` (the
value is the coordinate)::

    Plot(..., x=scale.Time("Date", rotation=80), y=scale.Linear("Value"))

Picking the scale type explicitly is deliberate.  ``dkit.plot`` forced every
axis onto integer positions with a ``FuncFormatter``, so real time, numeric and
log axes were unreachable and true x values were discarded.
"""
from dataclasses import dataclass, replace as _replace
from datetime import date, datetime
from typing import Any, ClassVar, Sequence, Union

from matplotlib.dates import AutoDateLocator, ConciseDateFormatter, DateFormatter
from matplotlib.ticker import (
    FixedFormatter, FixedLocator, MaxNLocator, PercentFormatter, StrMethodFormatter,
)

from ..exceptions import DKitPlotException


__all__ = ["Categorical", "Linear", "Log", "Percent", "Scale", "Time", "infer"]


@dataclass(frozen=True)
class Scale:
    """base class for axis scales: a continuous, unformatted axis

    args:
        label: axis label.  None draws no label.
        limits: ``(low, high)``.  None lets matplotlib autoscale.  Either entry
            may be None to autoscale that end only.
        rotation: tick label rotation in degrees
        format: tick label format.  Interpretation depends on the subclass; see
            each one.  None falls back to the theme, then to matplotlib.
        ticks: False draws the axis label but no ticks or tick labels.  This
            replaces ``dkit.plot``'s opaque ``XAxis(defeat=True)``.
        grid: True or False forces a grid on this axis, overriding the theme.
        nbins: maximum number of tick intervals.  None lets matplotlib decide.
    """
    label: Union[str, None] = None
    limits: Union[tuple, None] = None
    rotation: Union[float, None] = None
    format: Union[str, None] = None
    ticks: bool = True
    grid: Union[bool, None] = None
    nbins: Union[int, None] = None

    #: True if this scale maps discrete values onto positions
    discrete: ClassVar[bool] = False

    #: matplotlib scale name, applied with ``set_xscale`` / ``set_yscale``
    mpl_scale: ClassVar[str] = "linear"

    def replace(self, **kwargs) -> "Scale":
        """return a copy of this scale with the given fields changed"""
        return _replace(self, **kwargs)

    #
    # encoding
    #
    def encode(self, values: Sequence, domain: Union[Sequence, None] = None) -> list:
        """map raw field values to plot coordinates

        On a continuous scale the value *is* the coordinate, so this returns the
        values unchanged.  ``domain`` is ignored, and is only meaningful for
        discrete scales.
        """
        return list(values)

    #
    # axis configuration
    #
    def configure(self, ax, which: str = "x", theme=None,
                  domain: Union[Sequence, None] = None) -> None:
        """apply this scale to ``ax``

        args:
            ax: matplotlib Axes
            which: ``"x"`` or ``"y"``
            theme: :class:`~dkit.plot2.theme.Theme` supplying format defaults
            domain: ordered distinct values, for discrete scales
        """
        if which not in ("x", "y"):
            raise DKitPlotException(f"axis must be 'x' or 'y', not {which!r}")
        axis = ax.xaxis if which == "x" else ax.yaxis

        if self.mpl_scale != "linear":
            getattr(ax, f"set_{which}scale")(self.mpl_scale, **self._scale_kwargs())

        if self.label is not None:
            getattr(ax, f"set_{which}label")(self.label)

        locator = self._locator(domain)
        if locator is not None:
            axis.set_major_locator(locator)
        formatter = self._formatter(theme, domain, locator)
        if formatter is not None:
            axis.set_major_formatter(formatter)

        limits = self._limits(domain)
        if limits is not None:
            getattr(ax, f"set_{which}lim")(*limits)

        if not self.ticks:
            # keeps the axis label, drops ticks and tick labels
            axis.set_ticks([])

        if self.rotation:
            ax.tick_params(axis=which, labelrotation=self.rotation)
            if which == "x" and 0 < self.rotation < 180:
                for label in ax.get_xticklabels():
                    label.set_horizontalalignment("right")

        if self.grid is not None:
            ax.grid(self.grid, axis=which)

    #
    # hooks for subclasses
    #
    def _scale_kwargs(self) -> dict:
        """keyword arguments for ``set_xscale`` / ``set_yscale``"""
        return {}

    def _locator(self, domain):
        if self.nbins is not None:
            return MaxNLocator(self.nbins)
        return None

    def _formatter(self, theme, domain, locator):
        return None

    def _limits(self, domain):
        if self.limits is None:
            return None
        return self.limits


def _format_value(value: Any, fmt: Union[str, None]) -> str:
    """format one tick label with ``fmt``

    A single format applies to both dates and everything else: dates and
    datetimes go through ``strftime``, anything else through ``str.format``.
    This is what lets a categorical axis of month-ends be labelled ``"%b %Y"``.
    """
    if fmt is None:
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.strftime(fmt)
    try:
        return fmt.format(value, x=value)
    except (IndexError, KeyError, ValueError):
        return str(value)


@dataclass(frozen=True)
class Categorical(Scale):
    """discrete axis: one tick position per distinct value

    Positions come from the plot's *domain*, the distinct values contributed by
    every layer in order of first appearance, so two layers sharing a category
    land on the same position and the tick order does not vary between runs.

    ``format`` formats the tick labels: a ``strftime`` spec for date values, a
    ``str.format`` spec otherwise.

    args:
        pad: fraction of one category width left as margin at each end
    """
    pad: float = 0.5

    discrete: ClassVar[bool] = True

    def encode(self, values: Sequence, domain: Union[Sequence, None] = None) -> list:
        if domain is None:
            domain = list(dict.fromkeys(values))
        positions = {v: i for i, v in enumerate(domain)}
        try:
            return [positions[v] for v in values]
        except KeyError as e:
            raise DKitPlotException(f"value {e.args[0]!r} is not in the scale domain")

    def _locator(self, domain):
        return FixedLocator(list(range(len(domain or []))))

    def _formatter(self, theme, domain, locator):
        return FixedFormatter([_format_value(v, self.format) for v in (domain or [])])

    def _limits(self, domain):
        if self.limits is not None:
            return self.limits
        if not domain:
            return None
        return (-self.pad, len(domain) - 1 + self.pad)


@dataclass(frozen=True)
class Linear(Scale):
    """continuous numeric axis

    ``format`` is a ``str.format`` spec addressing the value as ``x``, e.g.
    ``"{x:,.0f}"``.  None falls back to ``Theme.number_format`` and then to
    matplotlib's own formatter.
    """

    def _formatter(self, theme, domain, locator):
        fmt = self.format or (theme.number_format if theme is not None else None)
        if fmt is None:
            return None
        return StrMethodFormatter(fmt)


@dataclass(frozen=True)
class Percent(Linear):
    """continuous axis labelled as a percentage

    args:
        whole: the value that represents 100%.  Use 1.0 for fractions, 100 for
            values already scaled to percent.
        decimals: digits after the decimal point.  None chooses automatically.
    """
    whole: float = 100.0
    decimals: Union[int, None] = 0

    def _formatter(self, theme, domain, locator):
        if self.format is not None:
            return StrMethodFormatter(self.format)
        return PercentFormatter(xmax=self.whole, decimals=self.decimals)


@dataclass(frozen=True)
class Log(Scale):
    """logarithmic axis

    args:
        base: logarithm base
    """
    base: float = 10.0

    mpl_scale: ClassVar[str] = "log"

    def _scale_kwargs(self) -> dict:
        return {"base": self.base}

    def _locator(self, domain):
        # a log axis has its own locator; MaxNLocator would defeat it
        return None

    def _formatter(self, theme, domain, locator):
        if self.format is None:
            return None
        return StrMethodFormatter(self.format)


@dataclass(frozen=True)
class Time(Scale):
    """date or datetime axis with genuine date ticks

    Values are plotted as dates, not as integer positions, so tick density and
    labels follow the range actually being plotted.

    ``format`` is a ``strftime`` spec.  None falls back to ``Theme.date_format``
    and then to matplotlib's concise date formatter.

    args:
        max_ticks: upper bound on the number of date ticks
    """
    max_ticks: int = 12

    def _locator(self, domain):
        return AutoDateLocator(maxticks=self.max_ticks)

    def _formatter(self, theme, domain, locator):
        fmt = self.format or (theme.date_format if theme is not None else None)
        if fmt is not None:
            return DateFormatter(fmt)
        return ConciseDateFormatter(locator)


def infer(values: Sequence, label: Union[str, None] = None, **kwargs) -> Scale:
    """pick a scale type from the values that will be plotted on it

    Used by :mod:`dkit.plot2.quick` to default an axis the caller did not
    specify.  Tier D never infers: :class:`~dkit.plot2.plot.Plot` requires the
    scale to be named, because guessing wrong on a long-lived plot
    specification is worse than being asked once.

    The rules, applied to the first value that is not None:

    ==================  ==================
    value type          scale
    ==================  ==================
    date, datetime      :class:`Time`
    str                 :class:`Categorical`
    bool                :class:`Categorical`
    anything else       :class:`Linear`
    ==================  ==================

    An empty sequence, or one holding only None, gives :class:`Linear`.

    args:
        values: the raw field values
        label: axis label for the returned scale
        **kwargs: passed to the chosen scale's constructor
    """
    sample = next((v for v in values if v is not None), None)
    if isinstance(sample, (date, datetime)):
        return Time(label, **kwargs)
    # bool before the numeric fallback: True and False read as categories
    if isinstance(sample, (str, bool)):
        return Categorical(label, **kwargs)
    return Linear(label, **kwargs)
