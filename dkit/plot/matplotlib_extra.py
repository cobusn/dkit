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
Matplotlib utility classes
"""
__all__ = ["SlopePlot"]

from collections import defaultdict
from functools import lru_cache
from typing import Dict, List
import matplotlib.pyplot as plt


class SlopePlot(object):
    """
    Create slope-plots that illustrate the difference between
    value across time.

    args:
        - series_field: defines series
        - pivot_field: field used as columns
        - value_field
    """
    def __init__(self, data, series_field, pivot_field, value_field, title, y_label,
                 pivots: List = None, float_fmt="{}", fig=None, ax=None):
        self.data = data
        self.sf = series_field
        self.pf = pivot_field
        self.vf = value_field
        self.title = title
        self.y_label = y_label
        self._pivot_label_font = {'size': 8, 'weight': 700}
        self._title_font = {'size': 10}
        self._y_label_font = {'size': 8}
        self._legend_font = {'size': 8}
        self._pivots = pivots
        self.float_fmt = float_fmt
        # Use supplied axis, if provided
        if ax:
            self.ax = ax
        else:
            fig, self.ax = plt.subplots(1, 1, figsize=(16, 8), dpi=80)
        self.fig = fig

    @property
    def _y_range(self):
        """minimum value of y"""
        return self._y_max - self._y_min

    @property
    def _y_min(self):
        """minimum value of y"""
        return min(self.values())

    @property
    def _y_max(self):
        return max(self.values())

    @lru_cache
    def series(self) -> Dict:
        """dict with serie values as keys and rows as values"""
        def get_row(row):
            return {k: row[k] for k in [self.sf, self.pf, self.vf]}

        serie_map = defaultdict(lambda: [])

        for row in self.data:
            serie_map[row[self.sf]].append(get_row(row))

        return serie_map

    @lru_cache
    def pivots(self) -> List:
        """set of pivot values"""
        if self._pivots:
            return self._pivots
        else:
            return sorted({r[self.pf] for r in self.data})

    @lru_cache
    def values(self) -> List:
        """list of values"""
        return [r[self.vf] for r in self.data]

    def label_data(self):
        """extract labels"""
        return [i[self.label_field] for i in self.data]

    def __scatter_points(self):
        """Draw Scatter points"""
        for i, rows in enumerate(self.series().values()):

            points = [i[self.vf] for i in rows]
            self.ax.scatter(
                y=points, x=[i+1 for i in range(len(points))], s=10, color='black', alpha=0.7
            )

    def _plot_it(self):
        for name, serie in self.series().items():
            pm = defaultdict(lambda: None, {r[self.pf]: r[self.vf] for r in serie})
            y = [pm[i] for i in self.pivots()]
            x = [i+1 for i in range(len(self.pivots()))]
            self.ax.plot(x, y)
            self.ax.scatter(x, y, s=10, color="black", alpha=0.7)

    def _legends(self):
        """add legends to first and last pivot columns"""
        p = self.pivots()
        left_pivot = p[0]
        right_pivot = p[-1]
        for c, serie in self.series().items():
            pm = defaultdict(
                lambda: defaultdict(lambda: None),
                {r[self.pf]: r for r in serie}
            )

            # left hand legend
            p1 = pm[left_pivot][self.vf]
            if p1:
                self.ax.text(
                    1-0.05, p1, c[:16] + ', ' + self.float_fmt.format(p1),
                    horizontalalignment='right',
                    verticalalignment='center',
                    fontdict=self._legend_font
                )

            # right hand legend
            rpos = len(self.pivots()) + 0.05
            p2 = pm[right_pivot][self.vf]
            if p2:
                self.ax.text(
                    rpos,  p2, c[:16] + ', ' + self.float_fmt.format(p2),
                    horizontalalignment='left',
                    verticalalignment='center',
                    fontdict=self._legend_font
                )

    def _vertical_lines(self):
        """draw vertical lines"""
        for i in range(len(self.pivots())):
            self.ax.vlines(
                x=i + 1, ymin=self._y_min,  ymax=self._y_max,  color='black',
                alpha=0.7, linewidth=1,
                linestyles='dotted'
            )

    def _pivot_headings(self):
        """draw pivot headings"""
        for i, pivot_name in enumerate(self.pivots()):
            self.ax.text(
                i + 1,
                self._y_min - 0.025 * self._y_range,
                pivot_name,
                horizontalalignment='center',
                verticalalignment='top',
                fontdict=self._pivot_label_font
            )

    def _decorate(self):
        self.ax.set_title(self.title, fontdict=self._title_font)

        # disable the xticks as pivot headings are on top
        # self.ax.set_xticks([i+1 for i in range(len(self.pivots()))])
        # self.ax.set_xticklabels(self.pivots())
        self.ax.set_xticks([])

        if self.y_label:
            self.ax.set_ylabel(self.y_label, fontdict=self._y_label_font)

        # Y ticks
        plt.yticks([self._y_min, self._y_max])
        # self.ax.tick_params(axis='x', labelsize=8)

        # remove borders
        self.ax.spines["top"].set_visible(False)
        self.ax.spines["bottom"].set_visible(False)
        self.ax.spines["right"].set_visible(False)
        self.ax.spines["left"].set_visible(False)

    def _set_limits(self):
        """calculate and set plot limits"""
        dy = (self._y_max - self._y_min) * .1
        dx = len(self.pivots()) + 1
        self.ax.set(xlim=(0, dx), ylim=(self._y_min-dy, self._y_max+dy))

    def draw(self):
        self._set_limits()
        self._vertical_lines()
        self._plot_it()
        self._legends()
        self._pivot_headings()
        self._decorate()

    def save(self, filename):
        self.fig.savefig(filename)
