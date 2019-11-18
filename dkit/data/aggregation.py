# Copyright (c) 2018 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, _modify, merge, publish, distribute, sublicense, and/or sell
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
Set of classes that assist in creating aggregated data from iterables of
dictionary like objects.abs

Generators are employed to ensure aggregation is done without
requiring the data to be loaded in Memory.
"""
from abc import ABC
from collections import defaultdict
from operator import attrgetter, itemgetter
from typing import List, Dict

from .. import exceptions, messages
from .stats import Accumulator


class Aggregate(object):
    """
    aggregation control object

    Args:
        * data: iterator of dictionary like objects
    """
    def __init__(self, data=None):
        self.data = data
        self.groupby_keys: List[str] = []
        self.accumulators: Dict[str, Accumulator] = {}
        self.aggregations: List["AbstractAggregation"] = []
        self.sorter: "OrderBy" = None

    def __add__(self, other):
        # test for correct lineage
        if not isinstance(other, AbstractModifier):
            raise exceptions.CkitDataException(messages.MSG_0016)

        other._modify(self)
        return self

    def _feed_data(self, data):
        for row in data:
            key = tuple(row[k] for k in self.groupby_keys)
            for target, accumulator in self.accumulators.items():
                accumulator[key].push(row[target])

    @property
    def required_fields(self):
        """List of fields required by aggregator"""
        return self.groupby_keys + list(self.accumulators.keys())

    @property
    def key_tuples(self):
        """list if key tuples"""
        first_aggregator = next(iter(self.accumulators.values()))
        return first_aggregator.keys()

    def _iter_result(self):
        for key_tuple in self.key_tuples:
            row = {k: key_tuple[i] for i, k in enumerate(self.groupby_keys)}
            for aggregation in self.aggregations:
                accumulator = self.accumulators[aggregation.target][key_tuple]
                row[aggregation.display_name] = aggregation.get_value(accumulator)
            yield(row)

    def __iter__(self):
        self._feed_data(self.data)
        if self.sorter is not None:
            yield from sorted(
                self._iter_result(),
                key=self.sorter.sort_keys,
                reverse=self.sorter.descending
            )
        else:
            yield from self._iter_result()

    def __call__(self, data):
        self.data = data
        yield from self.__iter__()


class AbstractModifier(ABC):
    pass


class GroupBy(AbstractModifier):
    """
    define keys to group on

    args:
        *keys: keys to group on
    """
    def __init__(self, *keys: str):
        self.groupby_keys = keys

    def _modify(self, other):
        other.groupby_keys += self.groupby_keys


class OrderBy(AbstractModifier):
    """_modify sort order"""

    def __init__(self, *columns):
        self.__sort_keys = columns
        self.descending = False

    def _modify(self, other):
        # Sort order can only be defined once
        if other.sorter is not None:
            raise exceptions.CkitDataException(messages.MSG_0017)
        other.sorter = self

    def reverse(self):
        """descending order"""
        self.descending = True
        return self

    @property
    def sort_keys(self):
        """key getter for sort algorithm"""
        return itemgetter(*self.__sort_keys)


class AbstractAggregation(AbstractModifier):

    function = "None"
    abbreviation = "none"

    def __init__(self, target: str = ""):
        self.target: str = target
        self.alias_name: str = None
        self._getter = attrgetter(self.function)

    def alias(self, alias: str):
        self.alias_name = alias
        return self

    def _modify(self, other: GroupBy):
        # Add accumulator
        if self.target not in other.accumulators:
            other.accumulators[self.target] = defaultdict(lambda: Accumulator())

        # Add output
        other.aggregations.append(self)

    @property
    def display_name(self,):
        """name displayed in output"""
        if not self.alias_name:
            return f"{self.abbreviation}_{self.target}"
        else:
            return self.alias_name

    def get_value(self, accumulator: Accumulator):
        """
        return appropriate value from accumulator
        """
        return self._getter(accumulator)


class Count(AbstractAggregation):
    """number of observations"""
    function = "observations"
    abbreviation = "count"


class IQR(AbstractAggregation):
    """inter-quantile Range"""
    function = "iqr"
    abbreviation = "iqr"


class Max(AbstractAggregation):
    """maximum value"""
    function = "max"
    abbreviation = "max"


class Mean(AbstractAggregation):
    """minimum value"""
    function = "mean"
    abbreviation = "avg"


class Median(AbstractAggregation):
    """median value"""
    function = "median"
    abbreviation = "median"


class Min(AbstractAggregation):
    """minimum value"""
    function = "min"
    abbreviation = "min"


class Sum(AbstractAggregation):
    """sum of values"""
    function = "sum"
    abbreviation = "sum"


class Std(AbstractAggregation):
    """standard deviation"""
    function = "stdev"
    abbreviation = "std"


class Var(AbstractAggregation):
    """variance"""
    function = "variance"
    abbreviation = "var"


class Quantile(AbstractAggregation):
    """quantile at specified value"""
    function = "quantile"
    abbreviation = "q"

    def __init__(self, target, quantile):
        super().__init__(target)
        self.quantile_value = quantile
        rounded = round(self.quantile_value * 100)
        self.__display_name = f"{self.abbreviation}{rounded}_{self.target}"

    def get_value(self, accumulator):
        return accumulator.quantile(self.quantile_value)

    @property
    def display_name(self):
        return self.__display_name


MAP_NON_PARAMETRIC_FUNCTIONS = {
    "count": Count,
    "std": Std,
    "sum": Sum,
    "iqr": IQR,
    "mean": Mean,
    "median": Median,
    "min": Min,
    "max": Max,
    "var": Var,
}

MAP_PARAMETRIC_FUNCTIONS = {
    "quantile": Quantile,
}
