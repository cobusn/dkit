# Copyright (c) 2021 Cobus Nel
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
Tools to infer data types from data.  For text data such as CSV, the
library attempt to identify the types.
"""
import collections
import decimal
import datetime
import re
import statistics
import dateutil.parser
from itertools import islice
from .iteration import iter_sample
from typing import Dict, List, Iterable

__all__ = [
    "ExtractSchemaInline",
    "InferSchema",
    "infer_type"
]


Field = collections.namedtuple("Field", ["type", "size"])
TypeStats = collections.namedtuple(
    "TypeStats", ["type", "str_type", "dirty", "min", "max", "mean", "stdev"]
)


MONTH_NAME_PATTERN = re.compile(
    r"^\s*\d{1,2}\s+"
    r"(jan|january|feb|february|mar|march|apr|april|may|jun|june|"
    r"jul|july|aug|august|sep|sept|september|oct|october|nov|november|"
    r"dec|december)"
    r"\s+\d{2,4}\s*$",
    re.IGNORECASE
)
DATE_ONLY_PATTERNS = [
    (re.compile(r"^\s*\d{4}[-/]\d{1,2}[-/]\d{1,2}\s*$"), ["%Y-%m-%d", "%Y/%m/%d"]),
    (re.compile(r"^\s*\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\s*$"), ["%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y"]),
]
DATETIME_PATTERNS = [
    (
        re.compile(r"^\s*\d{4}-\d{1,2}-\d{1,2}[ T]\d{1,2}:\d{2}:\d{2}(\.\d+)?\s*$"),
        ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"],
    ),
    (
        re.compile(r"^\s*\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?\s*$"),
        ["%H:%M", "%H:%M:%S", "%H:%M:%S.%f"],
    ),
]


def _get_main_type(types):
    """attempt to extract the dominant type for a field"""
    # note the sequence is important
    seq = [str, bytes, datetime.datetime, datetime.date, decimal.Decimal, bool, float, int, None]
    for t in seq:
        if t in types:
            return t


def _try_parse_formats(value, formats, type_):
    for fmt in formats:
        try:
            datetime.datetime.strptime(value, fmt)
            return type_
        except ValueError:
            continue
    return None


def _infer_date_type(value):
    stripped = value.strip()

    if MONTH_NAME_PATTERN.match(stripped):
        inferred = _try_parse_formats(
            stripped,
            ["%d %b %Y", "%d %B %Y", "%d %b %y", "%d %B %y"],
            datetime.date
        )
        if inferred is not None:
            return inferred

    for pattern, formats in DATETIME_PATTERNS:
        if pattern.match(stripped):
            inferred = _try_parse_formats(stripped, formats, datetime.datetime)
            if inferred is not None:
                return inferred

    for pattern, formats in DATE_ONLY_PATTERNS:
        if pattern.match(stripped):
            inferred = _try_parse_formats(stripped, formats, datetime.date)
            if inferred is not None:
                return inferred

    return None


class ExtractSchemaInline(object):
    """
    Extract types from a iterable while the iterable
    can still be used as an iterable

    Does not consume the iterable and can be used
    to iterate through values
    """

    def __init__(self, data: Iterable, block_size: int = 2000):
        self._block_size = block_size
        self._block: List = []
        self._data = None
        self.schema = None  # data schema stored here
        self._run_(data)

    def _run_(self, data):
        """infer data types"""
        self._data = data
        self._block = islice(self._data, self._block_size)
        typemap = collections.defaultdict(lambda: [])
        for row in self._block:
            for k, v in row.items():
                typemap[k].append(infer_type(v))
        self.schema = {k: _get_main_type(v) for k, v in typemap.items()}

    def __iter__(self) -> Iterable:
        for item in self._block:
            yield item
        for item in self._data:
            yield item


class InferSchema(object):
    """
    infer schema for an iterable of  dictionary records.

    additional data is collected to describe the schema

    Arguments:
        - strict: if
    """
    __type_map = {
        None: "string",
        int: "integer",
        float: "float",
        bool: "boolean",
        str: "string",
        bytes: "binary",
        decimal.Decimal: "decimal",
        datetime.date: "date",
        datetime.datetime: "datetime",
    }

    def __init__(self, strict: bool = True):
        self.the_iterable = None
        self.strict: bool = strict
        self.data: Dict[str, List[type]] = {}
        """map data types to field name"""

        self.summary: Dict[str, TypeStats] = {}
        """
        map computed TypeStats to field name

        This attribute is only meaningful after __call__ have
        been called on a dataset
        """

        self.__num_rows: int = 0

    def __len__(self):
        """length of sample"""
        return self.__num_rows

    def __collect_type_stats(self, the_iterable, strict, p=1.0, stop=100) \
            -> Dict[str, List[type]]:
        """collect type statistics for each field in data"""
        row_counter = 0
        data = collections.defaultdict(lambda: {})
        for row in iter_sample(the_iterable, p, stop):
            for key, value in row.items():
                the_type = infer_type(value, strict)
                size = len(str(value))
                data[key][row_counter] = Field(the_type, size)
                row_counter += 1
        self.__num_rows = row_counter
        return data

    def __generate_summary(self) -> Dict[str, TypeStats]:
        """
        generate type summary as a dict of TypeStats
        """
        summary = {}

        for key, points in self.data.items():
            sizes = [point.size for point in points.values() if point.size is not None]
            _type = _get_main_type(set([point.type for point in points.values()]))

            try:
                _max = max(sizes)
            except ValueError:
                _max = 0
            try:
                _min = min(sizes)
            except ValueError:
                _min = 0
            try:
                _mean = statistics.mean(sizes)
            except statistics.StatisticsError:
                _mean = 0
            _str_type = self.__type_map[_type]
            try:
                _stdev = statistics.stdev(sizes)
            except statistics.StatisticsError:
                _stdev = 0
            _dirty = True if len(sizes) < self.__num_rows else False
            summary[key] = TypeStats(_type, _str_type, _dirty, _min, _max, _mean, _stdev)
        return summary

    def __call__(self, the_iterable, strict=False,  p=1, stop=100) -> Dict[str, type]:
        """
        Infer data types from the provided iterable

        Args:
            the_iterable:   an iterator of dictionaries
            strict:         remove commas from numbers if false
            p:              probability of evaluating a row
            stop:           stop after n rows
        """
        self.the_iterable = the_iterable
        self.data = self.__collect_type_stats(the_iterable, strict, p=p, stop=stop)
        self.summary = self.__generate_summary()
        return {key: points.type for key, points in self.summary.items()}


def infer_type(input, empty_str=None, strict=True):
    """
    provide data type for a supplied Python object

    if the input is not a string, it will return the data type if the object
    if the input is a string, then attempt to guess the datatype

    This is useful in dealing with data from CSV input of unknown schema

    Reference:

    * https://stackoverflow.com/questions/2103071/
              determine-the-type-of-a-value-which-is-represented-as-string-in-python
    * https://stackoverflow.com/questions/10261141/determine-type-of-value-from-a-string-in-python

    args:
        - input: the object under consideration
        - empty_str: type to produce for an empty string
        - strict: remove commas from numbers (e.g. 300,000)
    """

    if input is None:
        return None

    if type(input) is str:
        # if empty string return empty_str value
        if len(input) == 0:
            return empty_str

        # Parse int or float
        try:
            input = input.strip()
            if len(input) == 0:
                return empty_str

            if input.lower() in ["true", "false", "yes", "no"]:
                return bool

            num_input = input.replace(',', "") if not strict else input

            # Integer
            try:
                candidate = type(int(num_input))
                return int
            except ValueError:
                candidate = str

            # Float
            try:
                candidate = type(float(num_input))
                return float
            except ValueError:
                candidate = str

        except (ValueError, SyntaxError):
            candidate = str

        if candidate in (int, float, bool):
            return candidate
        else:
            inferred_date_type = _infer_date_type(input)
            if inferred_date_type is not None:
                return inferred_date_type
            return str
    else:
        return type(input)
