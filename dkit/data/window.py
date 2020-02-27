from abc import ABC
from collections import defaultdict, deque
from statistics import median
from scipy.stats import linregress
from ..compatibility import fmean, fsum

__all__ = [
    "MovingWindow",
    "Average",
    "Gradient",
    "Median",
    "Last",
    "Sum",
    "Max",
    "Min"
]


class MovingWindow(ABC):
    """
    Moving Window

    Args:
        lag: moving window size
        truncate: do not yield first n rows
    """

    def __init__(self, lag, truncate=False):
        self.lag = lag
        self.functions = []
        self._partition_by = None
        self.fields = set()
        self.truncate = truncate

    def partition_by(self, *fields):
        self._partition_by = fields
        return self

    def get_key(self, row):
        if self._partition_by is not None:
            return tuple(row[k] for k in self._partition_by)
        else:
            return 0

    def __call__(self, *sources):
        fields = set(i.field for i in self.functions)
        accumulators = defaultdict(lambda: defaultdict(lambda: deque(maxlen=self.lag)))

        for source in sources:
            for row in source:
                key = self.get_key(row)
                for field in fields:
                    acc = accumulators[key]
                    acc[field].append(row[field])
                updates = any([fn.update(acc, row) for fn in self.functions])
                if updates:
                    yield row
                elif not self.truncate:
                    yield row

    def __add__(self, other):
        other._modify_(self)
        return self


class AbstractWindowFunction(ABC):

    function = None
    prefix = None

    def __init__(self, field, alias=None, na=0):
        self.field = field
        self._alias = alias if alias else f"{field}_{self.prefix}"
        self.na = na
        self.lag = 0

    def alias(self, name):
        self._alias = name
        return self

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
            return False
        else:
            row[self._alias] = self.function(values)
            return True

    def _modify_(self, other):
        other.functions.append(self)
        self.lag = other.lag


class Average(AbstractWindowFunction):
    """calculate moving average

    args:
        - field: numeric field to calculate
        - alias: change name to
        - na: use this value as na (default 0)

    """
    prefix = "ma"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
            return False
        else:
            row[self._alias] = fmean(values)
            return True


class Gradient(AbstractWindowFunction):
    """calculate moving gradient

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    prefix = "gr"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
            return False
        else:
            row[self._alias] = linregress(range(self.lag), values)[0]
            return True


class Median(AbstractWindowFunction):
    """calculate median for moving window

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    prefix = "median"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
            return False
        else:
            row[self._alias] = median(values)
            return True


class Last(AbstractWindowFunction):
    """extraxt last value from window

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    prefix = "last"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        row[self._alias] = values[-1]
        if len(values) > 0:
            return False
        else:
            return True


class Sum(AbstractWindowFunction):
    """sum of values in window

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    function = fsum
    prefix = "sum"


class Max(AbstractWindowFunction):
    """max of values in window

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    function = max
    prefix = "max"


class Min(AbstractWindowFunction):
    """min of values in window

    args:
        - field: numeric field for calculation
        - alias: change name to
        - na: use this value as na (default 0)

    """
    function = min
    prefix = "min"
