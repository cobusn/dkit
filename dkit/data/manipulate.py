# Copyright (c) 2015 Cobus Nel
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
Data manipulation routines.
"""

# =========== =============== =================================================
# May 2015    Cobus Nel       Created
# 10 Nov 2016 Cobus Nel       Updated
# 12 Nov 2016 Cobus Nel       Added ReducePivot
# 27 Nov 2016 Cobus Nel       Added AttrDict
# 01 Dec 2016 Cobus Nel       Added merge
# 09 Dec 2016 Cobus Nel       Updated documentation
# 30 Jan 2017 Cobus Nel       Added iter_sample
# 30 Jan 2017 Cobus Nel       Added infer_type
# 31 Jan 2017 Cobus Nel       Added infer_types
# 06 May 2017 Coubs Nel       Added iter_add_id
# 27 Jun 2017 Cobus Nel       Cleanup + reduce_aggregate
# 24 Oct 2017 Cobus Nel       Added aggregates
# 19 Jun 2018 Cobus Nel       Moved AttrDict to containers
# 11 Sep 2019 Cobus Nel       Added iter_rename, iter_drop
# =========== =============== =================================================
from .. import _missing_value_
from ..decorators import deprecated
from ..utilities.introspection import is_list
import collections
import datetime
import dateutil.parser
import decimal
import itertools
import math
import operator
import random
import statistics
import sys
import typing
import uuid


__all__ = [
        "Indexer",
        "InferTypes",
        "KeyIndexer",
        "MultiKeyIndexer",
        "Pivot",
        "ReducePivot",
        "aggregate",
        "aggregates",
        "index",
        "infer_type",
        "iter_add_id",
        "iter_drop",
        "iter_rename",
        "iter_sample",
        "merge",
        "reduce_aggregate",
]


def reduce_aggregate(the_iterable, by_list, value_field, function=operator.add):
    """
    reducing aggregator

    IMPORTANT.  The function parameter must take two parameters
    and not list (e.g. sum will not work as it require an iterable)

    :param the_iterable: list of dictionaries used as source data
    :param by_list: list of fields to group on
    :param value_field: field to aggregate
    :param function: function to apply
    :rtype: list of dictionaries
    """
    heading = "value" if value_field in by_list else value_field
    key_dict = collections.defaultdict(lambda: 0)
    for row in the_iterable:
        key = tuple(row[k] for k in by_list)
        key_dict[key] = function(key_dict[key], row[value_field])

    for key, value in key_dict.items():
        retval = dict(zip(by_list, key))
        retval.update({heading: value})
        yield retval


def aggregate(the_iterable, by_list, value_field, function=sum):
    """
    Provide SQL Group-by like functionality on lists of dictionaries.

    similar to the R aggregate function.

    :param the_iterable: list of dictionaries used as source data
    :param by_list: list of fields to group on
    :param value_field: field to aggregate
    :param function: function to apply
    :rtype: list of dictionaries
    """
    key_dict = collections.defaultdict(lambda: [])

    for row in the_iterable:
        key = tuple(row[k] for k in by_list)
        key_dict[key].append(row[value_field])

    for key, values in key_dict.items():
        retval = dict(zip(by_list, key))
        retval.update({value_field: function(values)})
        yield retval


def aggregates(the_iterable, by_list, agg_list):
    """
    Provide SQL Group-by like functionality on lists of dictionaries.

    similar to the R aggregate function.

    :param the_iterable: list of dictionaries used as source data
    :param by_list: list of fields to group on
    :param agg_list: list of tuples in form [("name", "field", func),..]
    :rtype: iterator of dictionaries
    """
    key_dict = collections.defaultdict(lambda: [])

    for row in the_iterable:
        key = tuple(row[k] for k in by_list)
        key_dict[key].append(row)

    for key, values in key_dict.items():
        retval = dict(zip(by_list, key))
        retval.update(
            {a[0]: a[2]([i[a[1]] for i in values]) for a in agg_list}
        )
        yield retval


#
# merge
#
class _Merge(object):
    """
    implement merge logic
    """
    def __init__(self, left, right, by_l, by_r, all_l=False, all_r=False, backend=None,
                 null=_missing_value_):
        self.by_l = by_l
        self.by_r = by_r
        self.all_l = all_l
        self.all_r = all_r
        self.backend = backend
        self.null = null
        self.right = iter(right)
        self.left = iter(left)

        try:
            self.peek_r = next(self.right)
            self.i_r = itertools.chain([self.peek_r], self.right)
        except TypeError:
            self.peek_r = right[0]
            self.i_r = right

        try:
            self.peek_l = next(left)
            self.i_l = itertools.chain([self.peek_l], left)
        except TypeError:
            self.peek_l = left[0]
            self.i_l = left

    def __rename_dict(self):
        """
        build rename dictionary for right hand data
        """
        rename_dict = {}
        for right_key in self.peek_r.keys():
            _right_key = right_key
            while _right_key in self.peek_l.keys():
                _right_key = f"r.{_right_key}"
            rename_dict[right_key] = _right_key
        return rename_dict

    def inner_join(self):
        rename_dict = self.__rename_dict()
        by_l = self.by_l
        idx_r = index(self.i_r, self.by_r, backend=self.backend).process()
        for row in self.i_l:
            l_key = tuple(row[i] for i in by_l)
            for r_match in idx_r.get(l_key, []):
                row.update({rename_dict[k]: v for k, v in r_match.items()})
                yield row

    def left_join(self):
        null = self.null
        rename_dict = self.__rename_dict()
        by_l = self.by_l
        empty_right = {k: null for k in rename_dict.keys()}
        idx_r = index(self.i_r, self.by_r, backend=self.backend).process()
        for row in self.i_l:
            l_key = tuple(row[i] for i in by_l)
            for r_match in idx_r.get(l_key, [empty_right]):
                row.update({rename_dict[k]: v for k, v in r_match.items()})
                yield row

    def right_join(self):
        raise NotImplementedError

    def full_join(self):
        null = self.null
        rename_dict = self.__rename_dict()
        by_l = self.by_l
        empty_right = {k: null for k in rename_dict.keys()}
        empty_left = {k: null for k in self.peek_l.keys()}
        left_keys = set()
        idx_r = index(self.i_r, self.by_r, backend=self.backend).process()
        # left hand
        for row in self.i_l:
            l_key = tuple(row[i] for i in by_l)
            left_keys.add(l_key)
            for r_match in idx_r.get(l_key, [empty_right]):
                row.update({rename_dict[k]: v for k, v in r_match.items()})
                yield row
        # right hand
        for r_key in idx_r.keys():
            if r_key not in left_keys:
                right_hand_rows = idx_r[r_key]
                for row in right_hand_rows:
                    # _row = dict(empty_row)
                    _row = dict(empty_left)
                    _row.update({rename_dict[k]: v for k, v in row.items()})
                    yield _row

    def __iter__(self):
        # Join logic
        if (not self.all_l) and (not self.all_r):
            return self.inner_join()
        elif (self.all_l) and (not self.all_r):
            return self.left_join()
        elif self.all_l and self.all_r:
            return self.full_join()
        elif (not self.all_l) and self.all_r:
            return self.right_join()


def merge(left, right, by_l, by_r, all_l=False, all_r=False, backend=None,
          null=_missing_value_):
    """
    merge datasets similar to SQL joins.

    Args:
        * left: left (itereable) side of join
        * right: right (iterable) side fo join
        * by_l: list of fields to join on left side
        * by_r: list of fields to join on right side
        * all_l: include all rows from left side
        * all_r: include all rows from right side
        * backend: map backend for right hand side. if large data use a shelve

    Returns:
        generator of dictionaries
    """
    _by_left = by_l if is_list(by_l) else [by_l]
    _by_right = by_r if is_list(by_l) else [by_r]
    yield from _Merge(left, right, _by_left, _by_right, all_l, all_r, backend, null)


class Indexer(collections.MutableMapping):

    def __init__(self, backend=None):
        if backend is not None:
            self._store = backend
        else:
            self._store = dict()

    def __getitem__(self, key):
        return self._store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self._store[self.__keytransform__(key)] = value

    def __delitem__(self, key):
        del self._store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)

    def __keytransform__(self, key):
        return key

    def process(self):
        raise NotImplementedError


class KeyIndexer(Indexer):

    def __init__(self, the_iterable, index_field, backend=None):
        super().__init__(backend)
        self.the_iterable = the_iterable
        self.index_field = index_field
        self.__process()

    def __process(self):
        for row in self.the_iterable:
            try:
                self.__getitem__(row[self.index_field]).append(row)
            except KeyError:
                self.__setitem__(row[self.index_field], [row])
        return self

    @deprecated()
    def process(self):
        return self


class MultiKeyIndexer(Indexer):

    def __init__(self, the_iterable, index_fields, backend=None):
        super().__init__(backend)
        self.the_iterable = the_iterable
        self.index_fields = index_fields
        self.__process()

    def __process(self):
        indexes = self.index_fields
        for row in self.the_iterable:
            the_key = tuple(row[k] for k in indexes)
            try:
                self.__getitem__(the_key).append(row)
            except KeyError:
                self.__setitem__(the_key, [row])
        return self

    @deprecated()
    def process(self):
        """deprecated"""
        return self


def index(the_iterable, index_def, backend=None):
    """
    Return correct Index based on index_def.

    If `index_def` is an instance of list, tuple, set then a MultiKeyIndexer
    is returned. Else a KeyIndexer is returned.
    """
    if isinstance(index_def, (list, set, tuple)):
        return MultiKeyIndexer(the_iterable, index_def, backend=backend)
    else:
        return KeyIndexer(the_iterable, index_def, backend=backend)


class __PivotAbstract(object):
    """Abstract class for pivot classes"""

    def __init__(self, data, row_keys, col_key, value_key, function=sum,
                 missing=0, ensure_cols=None):
        self._data = data
        self._missing = missing
        self._row_keys = row_keys
        self._col_key = col_key
        self._value_key = value_key
        self._function = function
        self._col_keys = set(ensure_cols) if ensure_cols else set()
        self._ds = self._create()

    def _create(self):
        raise NotImplementedError

    def _pivot_headings_raw(self):
        """
        Pivot column headings. Raw values
        """
        return list(self._col_keys)

    #
    # Properties
    #

    @property
    def column_headings(self):
        """
        Column Headings in sorted order as specified by _reverse_cols in constructor
        """
        return list(itertools.chain([k for k in self._row_keys], self.pivot_headings))

    @property
    def row_headings(self):
        """
        List containing row headings.

        The list is **not** ordered.
        """
        return self._ds.keys()

    @property
    def pivot_headings(self):
        """
        list containing column headings for pivot variables only

        List is converted to string.
        """
        return [str(i) for i in self._pivot_headings_raw()]


class Pivot(__PivotAbstract):
    """
    Generate pivot tables

    Generate simple pivot tables for dictionary lists

    This class require functions that operate on
    iterables. For example, sum.

    This version of the class require more memory since
    lists of values are maintained.  However the advantage
    is that non commutative calculations (e.g. calculation of
    median) can be performed.


    Example usage of this class as below:

    .. include:: ../../examples/example_pivot.py
        :literal:

    The above script will produce this output:

    .. include:: ../../examples/example_pivot.out
        :literal:

    args:
        - data: list of dictionaries
        - row_keys: list of row keys
        - col_key: key for row to pivot to column
        - value_key: value to summarize for pivot
        - function: function for pivot. must work on lists
        - missing: value to substitute for _missing value
        - reverse_cols: reverse order of columns if True
        - ensure_cols: ensure these columns exist
    """
    def __init__(self, data, row_keys, col_key, value_key, function=sum,
                 missing=0, ensure_cols=None):
        super().__init__(data, row_keys, col_key, value_key,
                         function, missing, ensure_cols)

    #
    # Methods
    #
    def _create(self):
        """
        Create pivoted data structure
        """
        ds = {}
        for row in self._data:
            self._col_keys.add(row[self._col_key])
            key = tuple([row[k] for k in self._row_keys])
            if key in ds.keys():
                if row[self._col_key] in ds[key].keys():
                    ds[key][row[self._col_key]].append(row[self._value_key])
                else:
                    ds[key][row[self._col_key]] = [row[self._value_key]]
            else:
                ds[key] = {row[self._col_key]: [row[self._value_key]]}
        return ds

    def rows(self, function=None, missing=0):
        """
        Iterator for rows in pivot table.

        This _function can be used to recreate the pivot table
        using a different _function or _missing value.
        """
        function = function if function else self._function
        missing = missing if missing else self._missing
        row_keys = self._row_keys
        row_headings = self.row_headings
        col_headings = self._pivot_headings_raw()

        for row_key in row_headings:
            retval = dict(zip(row_keys, row_key))
            # retval = collections.OrderedDict(zip(row_keys, row_key))
            retval.update([(str(col_key), function(self._ds[row_key].get(col_key, [missing])))
                           for col_key in col_headings])
            yield retval

    def __iter__(self):
        """
        Provide __iter__ functionality
        """
        for row in self.rows():
            yield row


class ReducePivot(__PivotAbstract):
    """
    Generate pivot tables

    Generate simple pivot tables for dictionary lists.

    The ReducePivot class require functions that have at
    least two parameters.  for example operator.add or min

    This version of the class require less memory since
    values are updated when processed.

    Example usage of this class as below:

    .. include:: ../../examples/example_reduce_pivot.py
        :literal:

    The above script will produce this output:

    .. include:: ../../examples/example_reduce_pivot.out
        :literal:

    args:
        - data: list of dictionaries
        - row_keys: list of row keys
        - col_key: key for row to pivot to column
        - value_key: value to summarize for pivot
        - function: function for pivot. must work on lists
        - missing: value to substitute for _missing value
        - reverse_cols: reverse order of columns if True
        - ensure_cols: ensure these columns exist

    """
    def __init__(self, data, row_keys, col_key, value_key, function=operator.add,
                 missing=0, ensure_cols=None):
        super().__init__(data, row_keys, col_key, value_key,
                         function, missing, ensure_cols)

    def _create(self):
        """
        Create pivoted data structure
        """
        func = self._function
        ds = {}
        for row in self._data:
            self._col_keys.add(row[self._col_key])
            key = tuple([row[k] for k in self._row_keys])
            if key in ds.keys():
                if row[self._col_key] in ds[key].keys():
                    ds[key][row[self._col_key]] = func(ds[key][row[self._col_key]],
                                                       row[self._value_key])
                else:
                    ds[key][row[self._col_key]] = row[self._value_key]
            else:
                ds[key] = {row[self._col_key]: row[self._value_key]}
        return ds

    def __iter__(self):
        """
        Iterator for rows in pivot table.

        This _function can be used to recreate the pivot table
        using a different _function or _missing value.
        """
        missing = self._missing
        row_keys = self._row_keys
        row_headings = self.row_headings
        col_headings = self._pivot_headings_raw()

        for row_key in row_headings:
            # retval = collections.OrderedDict(zip(row_keys, row_key))
            retval = dict(zip(row_keys, row_key))
            retval.update([(str(col_key), self._ds[row_key].get(col_key, missing))
                           for col_key in col_headings])
            yield retval


def iter_add_id(the_iterator, key="uuid"):
    """
    Add UUID to each row
    :param the_iterator: iterator containing the rows in dictionary format
    :param key: key for uuid
    """
    for row in the_iterator:
        row[key] = str(uuid.uuid4())
        yield row


def iter_drop(the_iterator, fields):
    """Drop specirfied fields from each row """
    return (
        {k: v for k, v in row.items() if k not in fields}
        for row in the_iterator
    )


def iter_sample(the_iterator, p: float = 1,
                k: int = 0) -> typing.Generator[typing.Dict, None, None]:
    """
    generates samples from items in a generator

    Will sample between 1 and k values with probability p.
    When k=None, the function will continue sampling until reaching sys.maxint

    Args:
        the_iterable: an iterable
        p: probability of sampling an item
        k: stop when k is reached (sys.max_size if not defined)
    """
    k = k if k > 0 else (sys.maxsize - 1)
    i = 0
    rand = random.random
    for entry in the_iterator:
        if (rand() <= p):
            yield entry
            i += 1
            if i >= k:
                break


def iter_rename(input, rename_map):
    """rename fields in a dict

    Args:
        - input: input iterator (iter of Dicts)
        - rename_map: rename keys
    """
    for row in input:
        for k, v in rename_map.items():
            row[v] = row.pop(k)
        yield row


def infer_type(input, empty_str=None, strict=True):
    """
    infer data type from python string

    Attempt to infer the data type of input.  Input is assumed to be string.
    If input is a different type that type is returned. If input is an
    empty string, the empty_str value is returned.

    Reference:

    * https://stackoverflow.com/questions/2103071/
              determine-the-type-of-a-value-which-is-represented-as-string-in-python
    * https://stackoverflow.com/questions/10261141/determine-type-of-value-from-a-string-in-python

    :param empty_str: type of empty string
    :param strict: remove commas from numbers (e.g. 300,000)
    """

    if input is None:
        return None

    if type(input) == str:
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
            # Not a number check for dates
            try:
                date_type = dateutil.parser.parse(input)
                return type(date_type)
            except (ValueError, OverflowError):
                # Ok is a str after all
                return str
    else:
        return type(input)


class InferTypes(object):
    """
    Infer types for a list of dictionaries.

    Provide additional information.
    """
    Point = collections.namedtuple("Point", ["type", "size"])
    TypeStats = collections.namedtuple(
        "TypeStats", ["type", "str_type", "dirty", "min", "max", "mean", "stdev"]
    )
    type_map = {
        None: "string",
        int: "integer",
        float: "float",
        bool: "boolean",
        str: "string",
        decimal.Decimal: "decimal",
        datetime.date: "date",
        datetime.datetime: "datetime",
    }

    def __init__(self, strict=True):
        self.the_iterable = None
        self.strict = strict
        self.data = None
        self.summary = None
        self.num_rows = 0

    def get_main_type(self, types):
        # note the sequence is important
        seq = [str, datetime.datetime, datetime.date, decimal.Decimal, bool, float, int, None]
        for t in seq:
            if t in types:
                return t

    def _collect_stats(self, the_iterable, strict, p=1.0, stop=100):
        row_counter = 0
        data = collections.defaultdict(lambda: {})
        for row in iter_sample(the_iterable, p, stop):
            for key, value in row.items():
                the_type = infer_type(value, strict)
                size = len(str(value))
                data[key][row_counter] = self.Point(the_type, size)
                row_counter += 1
        self.num_rows = row_counter
        return data

    def _calc_summary(self):
        # summary = collections.OrderedDict()
        summary = dict()

        for key, points in self.data.items():
            sizes = [point.size for point in points.values() if point.size is not None]
            _type = self.get_main_type(set([point.type for point in points.values()]))

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
            _str_type = self.type_map[_type]
            try:
                _stdev = statistics.stdev(sizes)
            except statistics.StatisticsError:
                _stdev = 0
            _dirty = True if len(sizes) < self.num_rows else False
            summary[key] = self.TypeStats(_type, _str_type, _dirty, _min, _max, _mean, _stdev)
        return summary

    # dirty
    def __get_dirty(self):
        return False

    dirty = property(__get_dirty)

    def __call__(self, the_iterable, strict=False,  p=1, stop=100):
        """
        Infer data types from the provided iterable

        Args:
            the_iterable:   an iterator of dictionaries
            strict:         remove commas from numbers if false
            p:              probability of evaluating a row
            stop:           stop after n rows
        """
        self.the_iterable = the_iterable
        self.data = self._collect_stats(the_iterable, strict, p=p, stop=stop)
        self.summary = self._calc_summary()
        return {key: points.type for key, points in self.summary.items()}


def remainder(the_iterable, index_key, value_key, threshold=0.8, n=15, title="Remainder",
              frac=False):
    """return iterable where all values over threshold is grouped under remainder.abs
    Args:
        - the_iterable: iterable to process
        - index_key: key
        - value_key: value
    """
    def map_abs(i):
        x = dict(i)
        x[value_key] = abs(x[value_key])
        return x
    data = map(map_abs, the_iterable)
    data = list(sorted(data, key=lambda x: x[value_key], reverse=True))
    total = math.fsum((i[value_key] for i in data))
    f_total = threshold * total
    ctot = 0
    rtot = 0
    for i, row in enumerate(data):
        if ctot < f_total and i < n-1:
            yield {
                index_key: row[index_key],
                value_key: (row[value_key] / total) * 100 if frac else row[value_key]
            }
            ctot += row[value_key]
        else:
            rtot += (row[value_key] / total) * 100 if frac else row[value_key]
    yield {index_key: title, value_key: rtot}
