# Copyright (c) 2022 Cobus Nel
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
Exploratory Data Analysis
"""
from typing import Iterable, Dict, Any
from matplotlib import cm
from matplotlib.colors import hex2color
from abc import ABC
import datetime
import decimal
import matplotlib.pyplot as plt
import numpy as np
from .infer import ExtractSchemaInline


class Normalizer(object):
    """
    Normalize a set of data between 0.0 and 1.0
    """
    def __init__(self, max_, min_):

        if isinstance(max_, (datetime.datetime, datetime.date)):
            delta = datetime.timedelta(microseconds=1)
            self._max = max_ + delta
            self._min = min_ - delta
            self._diff = self._max - self._min
        else:
            delta = 0.0001
            self._max = float(max_) + delta
            self._min = float(min_) - delta
            self._diff = float(self._max) - float(self._min)

    def __call__(self, number) -> float:
        try:
            return (number - self._min) / self._diff
        except ZeroDivisionError:
            return 0
        except TypeError:
            return None


class StructureMap(ABC):

    _color_map_name = "Blues"
    _null_color_name = "#000000"

    def __init__(self, width: int, data: Iterable[Dict[str, Any]]):
        self.width = len
        self._data = data
        max_ = max(self.non_null)
        min_ = min(self.non_null)
        self._normalizer = Normalizer(max_, min_)
        uniq = self.unique
        cats = uniq if uniq < 256 else 256
        self._color_map = cm.get_cmap(self._color_map_name, cats)
        self._null_color = (0.0, 0.0, 0.0, 1)

    @property
    def unique(self):
        return len(set(self.non_null))

    @property
    def non_null(self):
        return [i for i in self.data if i]

    @property
    def data(self):
        return self._data

    def normalized_values(self):
        """return data normalized between 0 and 1"""
        return [self._normalizer(i) for i in self.data]

    def _mapped_color(self, value):
        """mapped color for normalised value, value"""
        return self._color_map(value)

    def sub_plot(self):
        """return sub_plot"""
        pass

    def color_list(self):
        return [
            self._mapped_color(i) if i else self._null_color
            for i in self.normalized_values()
        ]


class IntegerStructureMap(StructureMap):
    _color_map_name = "PuRd"


class FloatStructureMap(StructureMap):
    _color_map_name = "Wistia"


class DecimalStructureMap(StructureMap):
    _color_map_name = "Greys"


class DateTimeStructureMap(StructureMap):
    _color_map_name = "PuBu"


class DateStructureMap(StructureMap):
    _color_map_name = "Oranges"


class BooleanStructureMap(StructureMap):
    _color_map_name = None


class StringStructureMap(StructureMap):
    _color_map_name = "YlGn"

    @property
    def data(self):
        return [len(str(i)) if i else None for i in self._data]


class NoneStructureMap(StructureMap):

    def __init__(self, width: int, data: Iterable[Dict[str, Any]]):
        self.width = len
        self._data = data
        self._null_color = hex2color(self._null_color_name)

    def color_list(self):
        return [self._null_color for i in self.data]


_type_map = {
    None: NoneStructureMap,
    str: StringStructureMap,
    int: IntegerStructureMap,
    float: FloatStructureMap,
    bool: BooleanStructureMap,
    decimal.Decimal: DecimalStructureMap,
    datetime.date: DateStructureMap,
    datetime.datetime: DateTimeStructureMap,
}


class SchemaMap(object):

    def __init__(self, depth):
        self.depth = depth

    def _create_processors(self, data, width=80):
        """set up processors"""
        processors: Dict[str, StructureMap] = {}
        typemapper = ExtractSchemaInline()
        _types = typemapper(data)
        _data = list(typemapper)
        for name, _type in _types.items():
            this_data = [row[name] for row in _data]
            processors[name] = _type_map[_type](width, this_data)
        return processors

    def save_plot(self, the_file, data):
        """save plot to file

        args:
            the_file: string filename or file object
        """
        self._create_plot(data)
        self.fig.savefig(the_file)

    def _create_plot(self, data):
        """
        create plot
        """
        processors = self._create_processors(data)
        nrows = len(processors)

        figh = 0.35 + 0.15 + (nrows + (nrows-1)*0.1)*0.22
        self.fig, axes = plt.subplots(nrows=nrows, figsize=(6.4, figh))
        self.fig.subplots_adjust(top=1-.01/figh, bottom=.01/figh, left=0.2, right=0.99)

        axes[0].set_title("Structure Map")

        for ax, name in zip(axes, processors.keys()):
            processor = processors[name]
            _data = processor.color_list()
            data = np.stack((_data, _data))
            if name == "CURRAMOUNT":
                print(processor.data)
                print(processor.normalized_values())
                print(processor.color_list())
            ax.imshow(
                data,
                aspect='auto',
                # cmap=processor._color_map_name
            )
            ax.text(
                -.01, .5, name, va='center', ha='right', fontsize=10, transform=ax.transAxes
            )

        # Turn off *all* ticks & spines, not just the ones with colormaps.
        for ax in axes:
            ax.set_axis_off()
