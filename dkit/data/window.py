from abc import ABC
from collections import defaultdict, deque
from statistics import fmean, fsum, median
from scipy.stats import linregress


class MovingWindow(ABC):
    """
    Moving Window

    Args:
        lag: moving window size
        truncate: do not yield first n rows
    """
    def __init__(self, lag, truncate=True):
        self.lag = lag
        self.functions = []
        self._order_by = []
        self._partition_by = []
        self.fields = set()
        self.truncate = truncate

    def partition_by(self, *fields):
        self._partition_by = fields
        return self

    def order_by(self, fields):
        pass

    def get_key(self, row):
        if self._partition_by is not None:
            return tuple(row[k] for k in self._partition_by)
        else:
            return 0

    def __call__(self, *sources):
        fields = set(i.field for i in self.functions)
        accumulators = defaultdict(
            lambda: defaultdict(
                lambda: deque(maxlen=self.lag)
            )
        )

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

    def __init__(self, field, alias=None, na=None):
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
    prefix = "last"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        row[self._alias] = values[-1]
        if len(values) > 0:
            return False
        else:
            return True


class Sum(AbstractWindowFunction):
    function = fsum
    prefix = "sum"


class Max(AbstractWindowFunction):
    function = max
    prefix = "max"


class Min(AbstractWindowFunction):
    function = min
    prefix = "max"
