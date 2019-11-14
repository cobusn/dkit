from abc import ABC
from collections import defaultdict, deque
from statistics import fmean, fsum, median
from scipy.stats import linregress


class MovingWindow(ABC):

    def __init__(self, lag):
        self.lag = lag
        self.functions = []
        self._order_by = []
        self._partition_by = []
        self.fields = set()

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
                for fn in self.functions:
                    fn.update(acc, row)
                yield(row)

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
        else:
            row[self._alias] = self.function(values)

    def _modify_(self, other):
        other.functions.append(self)
        self.lag = other.lag


class Average(AbstractWindowFunction):
    prefix = "ma"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
        else:
            row[self._alias] = fmean(values)


class Gradient(AbstractWindowFunction):
    prefix = "gr"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
        else:
            row[self._alias] = linregress(range(self.lag), values)[0]


class Median(AbstractWindowFunction):
    prefix = "median"

    def update(self, accumulator, row):
        values = accumulator[self.field]
        if len(values) < self.lag:
            row[self._alias] = self.na
        else:
            row[self._alias] = median(values)


class Sum(AbstractWindowFunction):
    function = fsum
    prefix = "sum"


class Max(AbstractWindowFunction):
    function = max
    prefix = "max"


class Min(AbstractWindowFunction):
    function = min
    prefix = "max"
