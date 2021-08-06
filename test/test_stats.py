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
# =========== =============== =================================================
# 16 Feb 2015 Cobus Nel       initial version
# 26 Apr 2018 Cobus Nel       updated for iterators
# 29 Oct 2018 Cobus Nel       updated for TDigests
#  6 Nov 2018 Cobus Nel       added BufferAccumulator
# =========== =============== =================================================

import numpy as np
import unittest
import random
import sys
sys.path.insert(0, "..")  # noqa
from dkit.data.stats import BufferAccumulator, Accumulator
from dkit.data.histogram import Histogram


class AccumulatorTestAbstract(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        unittest.TestCase.setUp(cls)
        cls.accuracy = 0.01
        cls.n = 10000
        cls.s = 3      # Significant digits
        cls.values = [
            [random.random() for i in range(cls.n)],
            [i for i in range(cls.n)],
            [i**2 for i in range(cls.n)]
        ]
        for a in cls.values:
            random.shuffle(a)
        cls.a = [cls.test_accumulator(a, precision=5) for a in cls.values]


class TestBufferAccumulator(AccumulatorTestAbstract):

    @classmethod
    def setUpClass(cls):
        cls.test_accumulator = BufferAccumulator
        super().setUpClass()

    def test_str(self):
        """Test str value"""
        # Just test that no error is generated
        for a in self.values:
            _ = str(a)

    def test_min(self):
        """
        Test minimum value
        """
        for i in range(len(self.values)):
            self.assertEqual(self.a[i].min, min(self.values[i]))

    def test_max(self):
        """
        Test maximum value
        """
        for i in range(len(self.values)):
            self.assertEqual(self.a[i].max, max(self.values[i]))

    def test_mean(self):
        """
        Test mean value
        """
        for i in range(len(self.values)):
            self.assertAlmostEqual(self.a[i].mean, np.mean(self.values[i]), self.s)

    def test_iqr(self):
        """test estimated median value"""

        for i in range(len(self.values)):
            q75, q25 = np.percentile(self.values[i], [75, 25])
            iqr = q75 - q25
            acc = abs(1-self.a[i].iqr/iqr)
            self.assertLessEqual(acc, self.accuracy)

    def test_observations(self):
        """
        Test observations
        """
        for i in range(len(self.values)):
            self.assertEqual(len(self.values[i]), self.a[i].observations)

    def test_quantile(self):
        """test quantile estimation"""
        quantiles = [0.05, 0.2, 0.5, 0.9, 0.99]
        for q in quantiles:
            for i in range(len(self.values)):
                acc = abs(1 - self.a[i].quantile(q)/np.quantile(self.values[i], q))
                self.assertLessEqual(acc, self.accuracy)

    def test_standard_deviation(self):
        """
        Test standard deviation
        """
        for i in range(len(self.values)):
            acc = abs(1 - self.a[i].stdev / np.std(self.values[i]))
            self.assertLessEqual(acc, self.accuracy)

    def test_as_map(self):
        """
        Test to_dict() function
        """
        for i in range(len(self.values)):
            d = self.a[i].as_map()
            self.assertEqual(d["observations"], self.n)

    def test_iter(self):
        """
        test using class as iterator
        """
        for i in range(len(self.values)):
            a = self.test_accumulator()
            s = sum(a(self.values[i]))
            self.assertEqual(s, sum(self.values[i]))
            acc = abs(1 - self.a[i].stdev / np.std(self.values[i]))
            self.assertLessEqual(acc, self.accuracy)

    def test_consume(self):
        """
        test consuming a generator
        """
        for i in range(len(self.values)):
            a = self.test_accumulator()
            a.consume(self.values[i])
            acc = abs(1 - self.a[i].stdev / np.std(self.values[i]))
            self.assertLessEqual(acc, self.accuracy)


class TestAccumulator(AccumulatorTestAbstract):

    @classmethod
    def setUpClass(cls):
        cls.test_accumulator = Accumulator
        super().setUpClass()

    def test_histogram(self):
        """test auto binning"""
        for a in self.a:
            hist = Histogram.from_accumulator(a, 10, precision=2)
            # assert that all points are counted
            c = sum([i.count for i in hist.bins])
            self.assertEqual(c, self.n)

    def test_merge(self):
        a = Accumulator(i for i in range(1000))
        b = Accumulator(i for i in range(1000))
        c = a + b
        print(a.as_map())
        print(c.as_map())


if __name__ == "__main__":
    unittest.main()
