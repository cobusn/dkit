import sys; sys.path.insert(0, "..")     # noqa
from dkit.data import window as win
from itertools import cycle
import random


def gen_data(n=20):
    product = cycle("AB")
    for i in range(n):
        yield {
            "product": next(product),
            "count": 1,
            "value": i,
            "rand": random.random()
        }


data = list(gen_data(20))

w = win.MovingWindow(6).partition_by("product") \
    + win.Max("count", na=0) \
    + win.Average("value") \
    + win.Median("rand") \

for row in w(data):
    print(row)
