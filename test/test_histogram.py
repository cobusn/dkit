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
'''
Created on 16 Feb 2015

@author: Cobus
'''
import unittest
import sys; sys.path.insert(0, "..") # noqa
import random
import math
from math import exp
from dkit.data.histogram import (
    Histogram,
    _recommended_bin_count,
    binner,
)
from dkit.data.helpers import frange
from dkit.data.stats import Accumulator


class TestHistogram(unittest.TestCase):

    def setUp(self):
        """Set up test generator"""
        n = 100000
        self.tests = {
            "uniform": (random.uniform(-1, 1) for i in frange(-5, 5, 0.01)),
            "linear": range(n),
            "exponential": (exp(i) for i in frange(-5, 5, 0.01)),
        }

    def test_histogram_accumulator(self):
        for test in self.tests.values():
            a = Accumulator(test)
            h_data = Histogram.from_accumulator(a)
            print(str(h_data))

    def test_histogram_data(self):
        for test in self.tests.values():
            values = list(test)
            h_data = Histogram.from_data(values, 6)
            # all input values must be accounted for in the bin counts
            total = sum(b.count for b in h_data.bins)
            self.assertEqual(total, len(values))
            print(str(h_data))

    def test_histogram_data_preserves_explicit_subdecimal_bins(self):
        values = [0.011 + index * 0.001 for index in range(9)]

        histogram = Histogram.from_data(values, bins=10)

        self.assertEqual(len(histogram.bins), 10)
        self.assertEqual(sum(bin_.count for bin_ in histogram.bins), len(values))
        self.assertEqual(histogram.bins[0].left, min(values))
        self.assertEqual(histogram.bins[-1].right, max(values))

    def test_histogram_data_tukey_mode_summarises_outliers(self):
        values = list(range(100)) + [-500_000, 500_000]

        histogram = Histogram.from_data(values, bins=8, range_mode="tukey")

        self.assertEqual(len(histogram.bins), 10)
        self.assertEqual(histogram.bins[0].left, float("-inf"))
        self.assertEqual(histogram.bins[-1].right, float("inf"))
        self.assertEqual(histogram.bins[0].count, 1)
        self.assertEqual(histogram.bins[-1].count, 1)
        self.assertEqual(sum(bin_.count for bin_ in histogram.bins), len(values))

    def test_histogram_data_tukey_mode_handles_zero_iqr(self):
        values = [10] * 100 + [-1_000, 1_000]

        histogram = Histogram.from_data(values, bins=8, range_mode="tukey")

        self.assertEqual(len(histogram.bins), 3)
        self.assertEqual([bin_.count for bin_ in histogram.bins], [1, 100, 1])

    def test_histogram_data_rejects_unknown_range_mode(self):
        with self.assertRaises(ValueError):
            Histogram.from_data([1, 2, 3], range_mode="unknown")

    def test_binner(self):
        data = [{"value": v} for v in range(1000)]
        bins = binner(data, "value", bins=10)
        # all input rows must be accounted for in the bin counts
        total = sum(b["count"] for b in bins)
        self.assertEqual(total, len(data))
        # requested bin count is respected
        self.assertEqual(len(bins), 10)
        # bins are ordered by left boundary
        lefts = [b["left"] for b in bins]
        self.assertEqual(lefts, sorted(lefts))

    def test_recommended_bin_count_uses_freedman_diaconis(self):
        class Stats:
            observations = 1000
            iqr = 10

        result = _recommended_bin_count(Stats(), 0, 100)
        expected = math.ceil(100 / (2 * 10 / 1000 ** (1 / 3)))
        self.assertEqual(result, min(expected, 50))

    def test_recommended_bin_count_falls_back_to_sturges(self):
        class Stats:
            observations = 15
            iqr = 0

        self.assertEqual(
            _recommended_bin_count(Stats(), 0, 10),
            math.ceil(math.log2(15) + 1),
        )

    def test_accumulator_defaults_to_full_range(self):
        accumulator = Accumulator([1, 2, 3, 4, 100])
        histogram = Histogram.from_accumulator(accumulator, n=4)
        self.assertEqual(histogram.bins[0].left, 1)
        self.assertEqual(histogram.bins[-1].right, 100)

    def test_accumulator_supports_tukey_range(self):
        values = [1, 2, 3, 4, 5] * 20 + [1000]
        accumulator = Accumulator(values)
        histogram = Histogram.from_accumulator(
            accumulator,
            n=4,
            range_mode="tukey",
        )
        self.assertEqual(histogram.bins[-1].right, float("inf"))
        self.assertGreater(histogram.bins[-1].count, 0)

    def test_accumulator_rejects_unknown_range_mode(self):
        accumulator = Accumulator([1, 2, 3])
        with self.assertRaises(ValueError):
            Histogram.from_accumulator(accumulator, range_mode="unknown")

    def test_empty_accumulator_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "without observations"):
            Histogram.from_accumulator(Accumulator())
if __name__ == "__main__":
    unittest.main()
