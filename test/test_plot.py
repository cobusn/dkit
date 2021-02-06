# Copyright (c) 2019 Cobus Nel
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

import sys; sys.path.insert(0, "..")  # noqa
from unittest import TestCase, main
from pathlib import Path

from dkit.plot import ggrammar
from dkit.plot.gnuplot import BackendGnuPlot
from dkit.plot.matplotlib import MPLBackend
from dkit.plot.plotly import PlotlyBackend
from sample_data import plot_data, scatter_data, histogram_data, control_chart_data


class AbstractPlot(object):

    @classmethod
    def setUpClass(cls):
        out_path = Path.cwd() / "plots"
        if not out_path.exists():
            print("Creating plots folder")
            out_path.mkdir()
        cls.out_path = out_path

    def gen_plt(self, data):
        plt = ggrammar.Plot(data) \
            + ggrammar.Title("2018 Sales") \
            + ggrammar.YAxis("Rand") \
            + ggrammar.XAxis("Month", rotation=70) \
            + ggrammar.Aesthetic(stacked=True, width=15, height=10)

        return plt

    def test_area_plot(self):
        """test area plot"""
        plt = self.gen_plt(plot_data)
        plt += ggrammar.GeomArea("Revenue", "index", "revenue", color="#0000FF", alpha=0.8)
        self.render(plt, "area_plot.svg")

    def test_histogram_plot(self):
        """test histogram plot"""
        plt = ggrammar.Plot(histogram_data) \
            + ggrammar.GeomHistogram("random data") \
            + ggrammar.Title("Random Data Histogram") \
            + ggrammar.YAxis("Frequency") \
            + ggrammar.XAxis("bin")
        self.render(plt, "histogram_plot.svg")

    def test_bar_plot(self):
        """test bar plots"""
        plt = self.gen_plt(plot_data)
        plt += ggrammar.GeomBar("Revenue", "index", "revenue", alpha=0.6)
        self.render(plt, "bar_plot.svg")

    def test_scatter_plot(self):
        """test scatter plot"""
        plt = ggrammar.Plot(scatter_data) \
            + ggrammar.GeomScatter("Scatter Plot", "x", "y", alpha=0.6) \
            + ggrammar.Title("Random Scatter Plot") \
            + ggrammar.YAxis("Random Y") \
            + ggrammar.XAxis("Random X", rotation=70)

        self.render(plt, "scatter_plot.svg")


class TestGnuPlot(AbstractPlot, TestCase):

    def render(self, plt, file_name):
        BackendGnuPlot("svg").render(
            plt.as_dict(),
            file_name=self.out_path / f"gnuplot_{file_name}"
        )


class TestMatplotlib(AbstractPlot, TestCase):

    def test_line_plot(self):
        """test bar plots"""
        plt = self.gen_plt(plot_data)
        plt += ggrammar.GeomLine("Revenue", "index", "revenue", alpha=0.6)
        self.render(plt, "line_plot.svg")

    def test_fill_plot(self):
        """test fill plot"""
        plt = self.gen_plt(control_chart_data)
        plt += ggrammar.GeomFill("Control Chart", x_data="index", y_upper="upper",
                                 y_lower="lower")
        self.render(plt, "fill_plot.svg")

    def render(self, plt, filename):
        MPLBackend("svg").render(
            plt.as_dict(),
            file_name=self.out_path / f"mpl_{filename}"
        )


class TestPlotly(AbstractPlot, TestCase):

    def render(self, plt, filename):
        MPLBackend("svg").render(
            plt.as_dict(),
            file_name=self.out_path / f"plotly_{filename}"
        )


if __name__ == '__main__':
    main()
