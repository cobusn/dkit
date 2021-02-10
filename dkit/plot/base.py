from abc import ABC, abstractmethod
from ..data.filters import ExpressionFilter
from . import ggrammar


class Backend(ABC):
    """base class for plot backends"""

    def __init__(self, terminal="pdf", style_sheet=None):
        self.terminal = terminal
        self.style_sheet = style_sheet if style_sheet else {}
        self.data = None

    @abstractmethod
    def render(self, grammar, file_name):
        self.grammar = grammar
        self.data = grammar["data"]
        for i, row in enumerate(self.data):
            row["_index"] = i


class SharedBackend(Backend):
    """
    Shared functionality
    """
    def __init__(self, terminal="pdf", style_sheet=None):
        super().__init__(terminal, style_sheet)
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
        if self.style_sheet:
            self._apply_style()

    @abstractmethod
    def _apply_style(self):
        pass

    def get_x_values(self, series, name="x_data"):
        """x values. return series if x is None"""
        if series["x_data"] is not None:
            if "filter_" in series and series["filter_"]:
                f = ExpressionFilter(series["filter_"])
                return [r[series[name]] for r in filter(f, self.data)]
            else:
                return [r[series[name]] for r in self.data]
        else:
            return [i for i in range(len(self.data))]

    def get_y_values(self, series, name="y_data"):
        if "filter_" in series and series["filter_"]:
            f = ExpressionFilter(series["filter_"])
            return [r[series[name]] for r in filter(f, self.data)]
        else:
            return [r[series[name]] for r in self.data]
