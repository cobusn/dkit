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
from operator import itemgetter
from typing import List

from dataclasses import dataclass

from .containers import SortedCollection
from .stats import Accumulator
import warnings
import tabulate

__all__ = ['Histogram']


class LegacyHistogram(Accumulator):
    """
    Create histogram bins and frequencies from data

    This class will automatically maintain statistics about the data
    fed to it in addition to calculating histogram data.

    Args:
    hist_min - Minimum value for brackets.
    hist_max - Maximmum value for brackets.
    brackets - Number of brackets for Histogram (Optional)
    precision -  Precision used for rounding bracket boundaries (Optional)
    """
    def __init__(self, hist_min: float, hist_max: float, brackets: int = 10, precision: int = 5):
        """
        Constructor
        """
        warnings.warn(
            "LegacyHistogram is deprecated, please use Histogram instead.",
            DeprecationWarning
        )
        self.inf = float("inf")
        self.neg_inf = -1 * self.inf
        super().__init__([], precision)
        self.hist_min = float(hist_min)
        self.hist_max = float(hist_max)
        self.no_brackets = brackets
        self._bins = self.__bins()
        self._histogram = SortedCollection(self._bins, key=itemgetter(0))

    def __bins(self):
        """
        Calculate bin brackets.
        """
        stepsize = round((self.hist_max-self.hist_min)/self.no_brackets, self.precision)
        bins = [[self.neg_inf, 0]]
        current = self.hist_min + stepsize
        for i in range(self.no_brackets):
            bins.append([current, 0])
            current = round(current + stepsize, self.precision)
        return bins

    def push(self, value):
        """
        Feed a value to the histogram.

        :param value: add value added to counters.
        :type value: float/int
        """
        super().push(value)
        self._histogram.find_le(value)[1] += 1

    @property
    def bins(self):
        """
        List containing brackets.  The brackets are in the following format::

          [[-infinity, count], [bracket1, count] ... [bracket n, count]]

        :returns: List containing the bin data.Return bin data.
        """
        return self._bins[0:-1]


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

    @classmethod
    def from_accumulator(cls, accumulator: "Accumulator", n: int = 10,
                         precision=None) -> List[Bin]:
        """
        estimate frequency distribution

        args:
            * n: number of bins
            * precision: decimal precision. If not specified, will inherit from class
        returns:
            * List[Bin]
        """
        precision_ = precision if precision is not None else accumulator.precision
        getcontext().prec = precision_
        centroids = list(accumulator._tdigest.centroids())
        low_ = accumulator.median - 2.5 * accumulator.iqr
        high_ = accumulator.median + 2.5 * accumulator.iqr
        p = pow(10, precision_)
        if accumulator.min > low_:
            low = Decimal(math.floor(accumulator.min * p) / p)
        else:
            low = Decimal(low_)
        if accumulator.max < high_:
            high = Decimal(math.ceil(accumulator.max * p) / p)
        else:
            high = Decimal(high_)

        binw = Decimal((high - low) / n)
        bins = [Bin(Decimal(low+i*binw), Decimal(low + (i+1)*binw)) for i in range(n)]
        bin_idx = 0
        for c in centroids:
            if c.mean <= bins[bin_idx].right:
                bins[bin_idx].count += c.weight
            else:
                if bin_idx < n-1:
                    bins[bin_idx + 1].count += c.weight
                    bin_idx += 1
                else:
                    bins[bin_idx].count += c.weight
        # print("-> ", sum(c.weight for c in centroids))
        return Histogram(bins)

    def _bin_width(self):
        """
        calculate bin width for histograms

        Using Freedman-Diaconis rule
        """
        n = self.observations
        return 2 * self.iqr/(math.pow(n, -1/3))
