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

"""
Benchmarking Utilities

    def _a():
        sum(range(1000))

    def _b():
        s = 0
        for i in range(1000):
            s += i

    results = benchmark((_a, _b), 500, 1000)
    print(results.table())

"""
import time
from boltons.statsutils import Stats
from tqdm import tqdm
from typing import Dict, List, Union


class BenchmarkResult(object):
    """
    Helper class to display and interpret benchmark results

    args:
        results: Dictionary of benchmark results
    """
    def __init__(self, results: Dict[str, Stats]):
        self.results = results

    def ordered(self):
        """dictionary ordered with slowest result first"""
        return {
            k: v for k, v in sorted(self.results.items(), key=lambda x: x[1].mean, reverse=True)
        }

    def table(self) -> List[Dict[str, Union[float, None]]]:
        """list of dictionaries with results"""
        table = [
            {
                "function": k,
                "mean": v.mean,
                "median": v.median,
            }
            for k, v in self.ordered().items()
        ]
        # calculate diff
        prev = None
        first = None
        for row in table:
            if prev:
                row["delta"] = prev - row["median"]
                row["times"] = prev / row["median"]
                row["times(0)"] = first / row["median"]
            else:
                first = row["median"]
            prev = row["median"]

        return table


def benchmark(functions, i=500, samples=100):
    """
    Benchmark function execution times (in nanoseconds)

    args:
        * fn: list of functions to call
        * i: number of calls to function per sample
        * samples: number of samples of i runs

    returns:
        * result dictionary
    """
    def _stat(fn_):
        t0 = time.process_time_ns()
        for i in range(samples):
            fn_()
        return time.process_time_ns() - t0

    return BenchmarkResult(
        {
            f.__name__: Stats(_stat(f) for iter in tqdm(range(i)))
            for f in functions
        }
    )
