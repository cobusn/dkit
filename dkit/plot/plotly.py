from .base import SharedBackend
from . import ggrammar
# from io import BytesIO

import pprint
pp = pprint.PrettyPrinter(indent=4)


class PlotlyBackend(SharedBackend):
    """Matplotlib Plot Render Backend

    Render plots from json grammar using the Matplotlib library

    Args:
        - grammar_instance: Json grammar (in dictionary format)
        - terminal
        - style_sheet: dictionary of style settings to apply

    """
    def __init__(self, grammar_instance, terminal="pdf", style_sheet=None):
        super().__init__(grammar_instance, terminal, style_sheet)
        self.render_map = {
            "anchoredtext": self.anchored_text,
            "geomarea": self.r_area_plot,
            "geomcumulative": self.r_cumulative_series,
            "geombar": self.r_bar_plot,
            "geomdelta": self.r_delta_plot,
            "geomfill": self.r_fill_plot,
            "geomheatmap": self.r_heatmap_plot,
            "geomhistogram": self.r_hist_plot,
            "geomline": self.r_line_plot,
            "geomscatter": self.r_scatter_plot,
            "geomtreemap": self.r_treemap_plot,
            "hline": self.r_hline,
            "vline": self.r_vline,
        }
        self.aes: ggrammar.Aesthetic = ggrammar.Aesthetic.from_dict(
            grammar_instance["aes"]
        )
        self.red_color = "#da291c"
        self.green_color = "#006747"
        if self.style_sheet:
            self._apply_style()

    def _apply_style(self):
        """Apply settings specified in stylesheet"""
        pass

    def set_title(self, ax):
        """set plot title"""
    pass

    def get_z_label(self, ax):
        """set z axis label"""
        pass

    def set_x_label(self, ax):
        """set x axis label"""
        pass

    def set_y_label(self, ax):
        """set y axis label"""
        axes = self.grammar["axes"]["1"]
        if "title" in axes:
            # ax.set_ylabel(self.grammar["axes"]['1']["title"])
            pass

        # string formatter
        if "float_format" in axes and axes["float_format"]:
            # ax.yaxis.set_major_formatter(StrMethodFormatter(axes["float_format"]))
            pass

    def set_labels(self, ax):
        """convenience function to set both axis labels"""
        self.set_x_label(ax)
        self.set_y_label(ax)

    def set_x_ticks(self, ax, hist=False):
        """X ticks and labels"""
        pass

    def get_color(self, series):
        if "color" in series and series["color"]:
            return series["color"]
        else:
            return None

    def get_line_style(self, series):
        if "line_style" in series:
            return series["line_style"]
        else:
            return None

    def anchored_text(self, ax, serie):
        """Add anchored text"""
        pass

    def _render_fig(self):
        pass

    def render(self, file_name):
        pass

    def render_mem(self, _format="PDF"):
        """
        render plot as BytesObject and return
        """
        pass

    def r_cumulative_series(self, ax, serie):
        pass

    def r_hline(self, ax, serie):
        """draw horizontal line"""
        pass

    def r_vline(self, ax, serie):
        """draw vertical line"""
        pass

    def r_fill_plot(self, ax, serie):
        """two lines with area between filled"""
        pass

    def r_area_plot(self, ax, serie):
        pass

    def r_hist_plot(self, ax, serie):
        pass

    def r_bar_plot(self, ax, serie, hist=False):
        pass

    def r_delta_plot(self, ax, serie):
        """bar plot with positive values greeen and negative red"""
        pass

    def r_heatmap_plot(self, ax, serie):
        """heatmap"""
        pass

    def r_line_plot(self, ax, serie):
        """render line series"""
        pass

    def r_scatter_plot(self, ax, series):
        """render scatter plot"""
        pass

    def r_treemap_plot(self, ax, serie):
        pass

    def save(self, fig, file_name):
        pass

    def apply_aesthetics(self):
        pass
