#
# Copyright (C) 2014  Cobus Nel
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#

"""
Abstraction of Histogram to assist with generating and plotting
of frequency plots.
"""
import math
from decimal import Decimal, getcontext
from typing import List

from dataclasses import dataclass

from .stats import Accumulator
import tabulate
import numpy

__all__ = ["Histogram", "binner"]


def _histogram_counts(data, bins=None, bin_digits=1):
    """compute (left, count) fixed-width histogram bins plus data max

    args:
        * data: list of numeric values
        * bins: number of bins, or None to auto-select via the
          Freedman-Diaconis rule
        * bin_digits: number of digits used to round down bin boundaries

    returns:
        * tuple of (list of (left, count) pairs, max value)
    """
    counts, edges = numpy.histogram(data, bins=bins if bins else "fd")
    factor = 10 ** bin_digits
    rounded_edges = [math.floor(e * factor) / factor for e in edges[:-1]]
    bin_counts = []
    for left, count in zip(rounded_edges, counts):
        if bin_counts and bin_counts[-1][0] == left:
            bin_counts[-1] = (left, bin_counts[-1][1] + int(count))
        else:
            bin_counts.append((left, int(count)))
    return bin_counts, float(edges[-1])


def binner(data, value_field, bins: int = None, bin_digits: int = 1):
    """
    Generate bin data for histogram data manipulation

    Args:

    * data: iterator for data
    * value_field: field name for the value field to be binned
    * bins: number of bins
    * bin_digits: round to this number of digits

    Returns:
        * list of dicts in format {"left": value, "count": value}

    """
    values = [i[value_field] for i in data]
    bin_counts, _ = _histogram_counts(values, bins=bins, bin_digits=bin_digits)
    return [{"left": left, "count": count} for left, count in bin_counts]


def _recommended_bin_count(accumulator, low, high, max_bins=50):
    """Estimate a readable bin count from accumulator statistics.

    Args:
        accumulator: Accumulator containing observations and IQR statistics.
        low: Lower bound of the histogram range.
        high: Upper bound of the histogram range.
        max_bins: Maximum number of automatically selected bins.

    Returns:
        Recommended number of histogram bins.
    """
    observations = accumulator.observations
    data_range = float(high - low)
    if observations < 2 or data_range <= 0:
        return 1

    iqr = float(accumulator.iqr)
    if iqr > 0:
        bin_width = 2 * iqr / observations ** (1 / 3)
        estimated = math.ceil(data_range / bin_width)
    else:
        estimated = math.ceil(math.log2(observations) + 1)

    return min(max(1, estimated), max_bins)


def _histogram_range(accumulator, range_mode):
    """Return the requested histogram range for an accumulator."""
    if accumulator.observations == 0:
        raise ValueError("cannot create a histogram without observations")

    minimum = float(accumulator.min)
    maximum = float(accumulator.max)
    if range_mode == "full":
        return minimum, maximum
    if range_mode == "tukey":
        iqr = float(accumulator.iqr)
        lower = float(accumulator.quantile(0.25)) - 1.5 * iqr
        upper = float(accumulator.quantile(0.75)) + 1.5 * iqr
        return max(minimum, lower), min(maximum, upper)
    raise ValueError("range_mode must be 'full' or 'tukey'")


def _tukey_fences(values):
    """Return finite Tukey fences that enclose the central distribution.

    Args:
        values: Numeric observations.

    Returns:
        Lower and upper fences, clamped to the observed range, followed by a
        flag indicating that the interquartile range is zero.
    """
    array = numpy.asarray(values, dtype=float)
    minimum = float(array.min())
    maximum = float(array.max())
    lower_quartile, upper_quartile = numpy.quantile(array, [0.25, 0.75])
    interquartile_range = upper_quartile - lower_quartile
    lower = max(minimum, float(lower_quartile - 1.5 * interquartile_range))
    upper = min(maximum, float(upper_quartile + 1.5 * interquartile_range))

    has_zero_iqr = interquartile_range == 0
    if lower == upper and minimum != maximum:
        distances = [abs(value - lower) for value in array if value != lower]
        half_width = min(distances) / 2
        lower = max(minimum, lower - half_width)
        upper = min(maximum, upper + half_width)
    elif lower == upper:
        lower -= 0.5
        upper += 0.5
    return lower, upper, has_zero_iqr


@dataclass
class Bin:
    """
    Represent a histogram bin
    """
    left: float
    right: float
    count: int = 0

    @property
    def width(self):
        return self.right - self.left

    @property
    def midpoint(self):
        return (self.left + self.right) / 2

    def as_dict(self):
        return {
            "left": self.left,
            "right": self.right,
            "midpoint": self.midpoint,
            "count": self.count,
            "width": self.width,
        }


class Histogram(object):
    """
    Histogram object used to standardise creating histograms from
    various different data structures.
    """
    def __init__(self, bins: List[Bin]):
        self.bins = bins

    def __iter__(self):
        return (i.as_dict() for i in self.bins)

    def __str__(self):
        return tabulate.tabulate(
            [i.as_dict() for i in self.bins],
            headers="keys"
        )

    def plot_data(self):
        """Return list of dicts for plotting"""
        return [b.as_dict() for b in self.bins]

    @classmethod
    def from_data(cls, data, bins: int = None, precision=1,
                  range_mode="full") -> "Histogram":
        """Create a fixed-width Freedman-Diaconis histogram.

        Args:
            data: Numeric values to group into bins.
            bins: Number of bins, or ``None`` to use Freedman-Diaconis.
            precision: Retained for compatibility. Bin edges are not rounded,
                because rounding can collapse distinct buckets.
            range_mode: ``full`` bins the entire observed range. ``tukey``
                creates low and high overflow bins outside Tukey fences.

        Returns:
            Histogram with the exact bin edges calculated by NumPy.
        """
        values = list(data)
        if range_mode == "full":
            counts, edges = numpy.histogram(
                values,
                bins=bins if bins else "fd",
            )
            return cls([
                Bin(float(left), float(right), int(count))
                for left, right, count in zip(edges[:-1], edges[1:], counts)
            ])
        if range_mode != "tukey":
            raise ValueError("range_mode must be 'full' or 'tukey'")

        lower, upper, has_zero_iqr = _tukey_fences(values)
        central_values = [value for value in values if lower <= value <= upper]
        if has_zero_iqr:
            counts = [len(central_values)]
            edges = [lower, upper]
        else:
            counts, edges = numpy.histogram(
                central_values,
                bins=bins if bins else "fd",
                range=(lower, upper),
            )
        histogram_bins = [
            Bin(float(left), float(right), int(count))
            for left, right, count in zip(edges[:-1], edges[1:], counts)
        ]
        low_outliers = sum(value < lower for value in values)
        high_outliers = sum(value > upper for value in values)
        if low_outliers:
            histogram_bins.insert(0, Bin(float("-inf"), lower, low_outliers))
        if high_outliers:
            histogram_bins.append(Bin(upper, float("inf"), high_outliers))
        return cls(histogram_bins)

    @classmethod
    def from_accumulator(cls, accumulator: "Accumulator", n: int = None,
                         precision=None, range_mode="full") -> "Histogram":
        """
        estimate frequency distribution

        args:
            * n: number of bins; automatically estimated when omitted
            * precision: decimal precision. If not specified, will inherit from class
            * range_mode: either ``full`` or ``tukey``
        returns:
            * List[Bin]
        """
        precision_ = precision if precision is not None else accumulator.precision
        getcontext().prec = precision_
        centroids = list(accumulator._tdigest.centroids())
        low_, high_ = _histogram_range(accumulator, range_mode)
        p = pow(10, precision_)
        low = Decimal(math.floor(low_ * p) / p)
        high = Decimal(math.ceil(high_ * p) / p)

        if low == high:
            low -= Decimal("0.5")
            high += Decimal("0.5")

        if n is None:
            n = _recommended_bin_count(accumulator, low, high)
        elif n < 1:
            raise ValueError("number of bins must be positive")

        binw = Decimal((high - low) / n)
        bins = [Bin(Decimal(low+i*binw), Decimal(low + (i+1)*binw)) for i in range(n)]
        bin_idx = 0
        underflow = 0
        overflow = 0
        for c in centroids:
            if c.mean < low:
                underflow += c.weight
            elif c.mean > high:
                overflow += c.weight
            else:
                while bin_idx < n - 1 and c.mean > bins[bin_idx].right:
                    bin_idx += 1
                bins[bin_idx].count += c.weight

        if range_mode == "tukey":
            if underflow:
                bins.insert(0, Bin(Decimal("-Infinity"), low, underflow))
            if overflow:
                bins.append(Bin(high, Decimal("Infinity"), overflow))
        return Histogram(bins)
