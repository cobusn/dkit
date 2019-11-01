from dkit.data.stats import Accumulator
from dkit.data.histogram import Histogram

from random import random

a = Accumulator((random() for i in range(10000)))
h = Histogram.from_accumulator(a, n=10, precision=2)
for bin_ in h.bins:
    print(bin_)
