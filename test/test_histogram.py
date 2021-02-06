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
from math import exp
from dkit.data.histogram import Histogram, LegacyHistogram
from dkit.data.helpers import frange
from dkit.plot import ggrammar
from dkit.plot.gnuplot import BackendGnuPlot
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

    def test_histogram(self):
        for name, test in self.tests.items():
            a = Accumulator(test)
            h_data = Histogram.from_accumulator(a)
            plt = ggrammar.Plot(h_data) \
                + ggrammar.Aesthetic(width=78, height=25) \
                + ggrammar.GeomHistogram(name, "#FF0000", 0.8) \
                + ggrammar.Title("Random Data Histogram") \
                + ggrammar.YAxis("frequency") \
                + ggrammar.XAxis("bin")
            print(BackendGnuPlot(terminal="svg").render_str(plt.as_dict()))
            print(str(h_data))


class TestLegacyHistogram(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self.values = []
        for i in range(10):
            [self.values.append(i) for j in range(10)]
        random.shuffle(self.values)

        self.h = LegacyHistogram(0, 10, 10)
        for i in self.values:
            self.h.push(i)

    def test_float(self):
        """
        Test histogram with float values.
        """
        h = LegacyHistogram(-1.0, 1.0, 10)
        iterations = 1000
        mu = 0.0
        sigma = 1.0
        for i in range(iterations):
            h.push(random.normalvariate(mu, sigma))

    def test_int(self):
        """
        Test histogram with integer values.
        """
        h = LegacyHistogram(-10, 10, 10)
        iterations = 1000
        for i in range(iterations):
            randval = random.randint(-10, 10)
            h.push(randval)

    def test_empty_histogram(self):
        """
        Test behaviour with no data.
        """
        h = LegacyHistogram(-10, 10, 10)
        self.assertEqual(h.mean, 0)
        self.assertEqual(h.variance, 0)


if __name__ == "__main__":
    unittest.main()
