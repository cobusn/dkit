import unittest
from pathlib import Path
import sys; sys.path.insert(0, "..")  # noqa
from dkit.plot import ggrammar
from dkit.plot.matplotlib import MPLBackend
from dkit.plot.gnuplot import BackendGnuPlot


from sample_data import plot_data, scatter_data, histogram_data, control_chart_data


class TestGnuplot(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        out_path = Path.cwd() / "plots"
        if not out_path.exists():
            print("Creating plots folder")
            out_path.mkdir()
        cls.out_path = out_path

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

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
        self.render(plt, self.out_path / "example_area_plot.svg")

    def test_histogram_plot(self):
        """test histogram plot"""
        plt = ggrammar.Plot(histogram_data) \
            + ggrammar.GeomHistogram("random data") \
            + ggrammar.Title("Random Data Histogram") \
            + ggrammar.YAxis("Frequency") \
            + ggrammar.XAxis("bin")
        self.render(plt, self.out_path / "example_histogram_plot.svg")

    def test_bar_plot(self):
        """test bar plots"""
        plt = self.gen_plt(plot_data)
        plt += ggrammar.GeomBar("Revenue", "index", "revenue", alpha=0.6)
        self.render(plt, self.out_path / "example_bar_plot.svg")

    def test_fill_plot(self):
        """test fill plot"""
        plt = self.gen_plt(control_chart_data)
        plt += ggrammar.GeomFill("Control Chart", x_data="index", y_upper="upper",
                                 y_lower="lower")
        self.render(plt, self.out_path / "example_fill_plot.svg")

    def test_scatter_plot(self):
        """test scatter plot"""
        plt = ggrammar.Plot(scatter_data) \
            + ggrammar.GeomScatter("Scatter Plot", "x", "y", alpha=0.6) \
            + ggrammar.Title("Random Scatter Plot") \
            + ggrammar.YAxis("Random Y") \
            + ggrammar.XAxis("Random X", rotation=70)
        self.render(plt, self.out_path / "example_scatter_plot.svg")

    def render(self, plt, file_name):
        BackendGnuPlot(plt.as_dict(), "svg").render(
            file_name=file_name
        )


class TestMatplotlib(TestGnuplot):

    def test_line_plot(self):
        """test bar plots"""
        plt = self.gen_plt(plot_data)
        plt += ggrammar.GeomLine("Revenue", "index", "revenue", alpha=0.6)
        self.render(plt, self.out_path / "example_line_plot.svg")

    def render(self, plt, filename):
        MPLBackend(plt.as_dict(), "svg").render(
            file_name=filename
        )


if __name__ == '__main__':
    unittest.main()
