# Copyright (c) 2019 Cobus Nel
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
source of test data for unittests
"""
from random import random, gauss
from dkit.data.stats import Accumulator
from dkit.data.histogram import Histogram


N = 1000

histogram_data = Histogram.from_accumulator(
    Accumulator((gauss(0, 1.0) for i in range(N))),
    precision=2
)


plot_data = [
    {"index": "jan", "sales": 1, "revenue": 0.1},
    {"index": "feb", "sales": 10, "revenue": 30},
    {"index": "mar", "sales": 13, "revenue": 25},
    {"index": "apr", "sales": 10, "revenue": 20},
    {"index": "may", "sales": 10, "revenue": 50},
    {"index": "jun", "sales": 10, "revenue": 20},
    {"index": "jul", "sales": 10, "revenue": 20},
]


scatter_data = [
    {"x": random(), "y": random()}
    for _ in range(N)
]
