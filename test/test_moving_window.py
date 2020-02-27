# Copyright (c) 2020 Cobus Nel
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


import unittest
import random
import sys
sys.path.insert(0, "..")  # noqa
from dkit.compatibility import fmean, fsum
from dkit.data import window as win
from statistics import median


class TestMovingWindow(unittest.TestCase):

    def setUp(self):
        self.testdata = self.gen()

    def gen(self):
        return [
            {
                "idx": i,
                "key": random.choice(["a", "b", "c", "d"]),
                "value": random.random(),
            }
            for i in range(30)
        ]

    def test_lag_na_default(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value")
        result = list(w(self.testdata))

        self.assertEqual(
            [result[i]["value_ma"] for i in range(4)],
            [0, 0, 0, 0]
        )

    def test_lag_na_specified(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value", na=None)
        result = list(w(self.testdata))

        self.assertEqual(
            [result[i]["value_ma"] for i in range(4)],
            [None, None, None, None]
        )

    def test_lag_na(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value")
        result = list(w(self.testdata))

        self.assertEqual(
            [result[i]["value_ma"] for i in range(4)],
            [0, 0, 0, 0]
        )

    def test_partition(self):
        w = win.MovingWindow(3, truncate=False).partition_by("key") \
            + win.Average("value")
        result = [i for i in list(w(self.testdata)) if i["key"] == "a"]
        ma = fmean([i["value"] for i in result[-3:]])
        self.assertEqual(
            ma,
            result[-1]["value_ma"]
        )

    def test_alias1(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value").alias("ma")
        result = list(w(self.testdata))

        ma = fmean([i["value"] for i in result[-5:]])
        self.assertEqual(
            ma,
            result[-1]["ma"]
        )

    def test_alias2(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value", alias="ma")
        result = list(w(self.testdata))

        ma = fmean([i["value"] for i in result[-5:]])
        self.assertEqual(
            ma,
            result[-1]["ma"]
        )

    def test_ma(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Average("value")
        result = list(w(self.testdata))

        ma = fmean([i["value"] for i in result[-5:]])
        self.assertEqual(
            ma,
            result[-1]["value_ma"]
        )

    def test_median(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Median("value")
        result = list(w(self.testdata))
        test = median([i["value"] for i in result[-5:]])
        self.assertEqual(
            test,
            result[-1]["value_median"]
        )

    def test_last(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Last("value")
        result = list(w(self.testdata))
        test = result[-1]["value"]
        self.assertEqual(
            test,
            result[-1]["value_last"]
        )

    def test_sum(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Sum("value")
        result = list(w(self.testdata))
        test = fsum([i["value"] for i in result[-5:]])
        self.assertEqual(
            test,
            result[-1]["value_sum"]
        )

    def test_max(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Max("value")
        result = list(w(self.testdata))
        test = max([i["value"] for i in result[-5:]])
        self.assertEqual(
            test,
            result[-1]["value_max"]
        )

    def test_min(self):
        w = win.MovingWindow(5, truncate=False) \
            + win.Min("value")
        result = list(w(self.testdata))
        test = min([i["value"] for i in result[-5:]])
        self.assertEqual(
            test,
            result[-1]["value_min"]
        )


if __name__ == '__main__':
    unittest.main()
