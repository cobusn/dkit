from .base import SharedBackend
from . import ggrammar
import plotly.graph_objects as go
from itertools import cycle
# from io import BytesIO
import pandas as pd
import pprint
pp = pprint.PrettyPrinter(indent=4)


DEFAULT_COLORS = [
    '#1f77b4',  # muted blue
    '#ff7f0e',  # safety orange
    '#2ca02c',  # cooked asparagus green
    '#d62728',  # brick red
    '#9467bd',  # muted purple
    '#8c564b',  # chestnut brown
    '#e377c2',  # raspberry yogurt pink
    '#7f7f7f',  # middle gray
    '#bcbd22',  # curry yellow-green
    '#17becf'   # blue-teal
]


class PlotlyBackend(SharedBackend):
    """Matplotlib Plot Render Backend

    Render plots from json grammar using the Matplotlib library

    Args:
        - grammar_instance: Json grammar (in dictionary format)
        - terminal
        - style_sheet: dictionary of style settings to apply

    """
    def __init__(self, terminal="pdf", style_sheet=None):
        self.width = None
        self.height = None
        self.colors = DEFAULT_COLORS
        self.palette = cycle(self.colors)
        super().__init__(terminal, style_sheet)

    def _apply_style(self):
        """Apply settings specified in stylesheet"""
        style = self.style_sheet["plotly"]
        if "theme" in style:
            self.theme = style["theme"]
        else:
            self.theme = None

        # width and height
        if "width" in style and "height" in style:
            self.width = style["width"]
            self.height = style["height"]
        else:
            self.width = self.height = None

        if "colors" in style:
            colors = style["colors"]
            if "palette" in colors:
                self.colors = colors["palette"]
                self.palette = cycle(self.colors)
            if "red" in colors:
                self.red_color = colors["red"]
            if "green" in colors:
                self.green_color = colors["green"]

    def set_title(self, fig):
        """set plot title"""
        fig.update_layout(title=self.grammar["title"])

    def set_z_label(self, ax):
        """set z axis label"""
        pass

    def set_x_label(self, fig):
        """set x axis label"""
        axes = self.grammar["axes"]["0"]
        if "title" in axes:
            fig.update_layout(xaxis_title=axes["title"])

    def set_size(self, fig):
        if self.width and self.height:
            fig.update_layout(height=self.height, width=self.width)

    def set_y_label(self, fig):
        """set y axis label"""
        axes = self.grammar["axes"]["1"]
        if "title" in axes:
            fig.update_layout(yaxis_title=axes["title"])

    def set_x_ticks(self, ax):
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

    def anchored_text(self, data, layout):
        """Add anchored text"""
        pass

    def initialise(self):
        data = []
        layout = go.Layout(
            # title=go.layout.Title(text="Election results", x=0.5),
            # yaxis_title="Seats",
            # xaxis_tickmode="array",
            # xaxis_tickvals=list(range(27)),
            # xaxis_ticktext=tuple(df['year'].values),
        )
        return data, layout

    def _render_fig(self):
        data, layout = self.initialise()
        for serie in self.grammar["series"]:
            renderer = self.render_map[serie["~>"]]
            renderer(serie, data, layout)
        fig = go.Figure(
            data=data,
            layout=layout
        )
        self.apply_aesthetics(fig)
        return fig

    def render(self, grammar, file_name):
        super().render(grammar, file_name)
        self.aes: ggrammar.Aesthetic = ggrammar.Aesthetic.from_dict(
            grammar["aes"]
        )
        fig = self._render_fig()
        self.save(fig, file_name)
        pass

    def render_mem(self, _format="PDF"):
        """
        render plot as BytesObject and return
        """
        pass

    def r_cumulative_series(self, data, layout):
        pass

    def r_hline(self, data, layout):
        """draw horizontal line"""
        pass

    def r_vline(self, data, layout):
        """draw vertical line"""
        pass

    def r_fill_plot(self, data, layout):
        """two lines with area between filled"""
        pass

    def __r_plot(self, plot_type, series, data: list, layout: list, **opts):
        if color := self.get_color(series):
            opts["marker_color"] = color
        else:
            opts["marker_color"] = next(self.palette)
        data.append(
            plot_type(
                x=self.get_x_values(series),
                y=self.get_y_values(series),
                **opts
            )
        )
        return data, layout

    def r_area_plot(self, series, data: list, layout: list):
        """area plot"""
        return self.__r_plot(
            go.Scatter, series, data, layout, mode="lines", fill="tozeroy"
        )

    def r_bar_plot(self, series, data: list, layout: list):
        """bar plot"""
        return self.__r_plot(go.Bar, series, data, layout)

    def r_hist_plot(self, series, data: list, layout: list):
        """histogram"""
        data.append(
            go.Bar(
                x=[r["midpoint"] for r in self.data],
                y=[r["count"] for r in self.data]
            )
        )
        return data, layout

    def r_line_plot(self, series, data: list, layout: list):
        """render line series"""
        return self.__r_plot(go.Scatter, series, data, layout, mode="lines")

    def r_scatter_plot(self, series, data: list, layout: list):
        """render scatter plot"""
        return self.__r_plot(go.Scatter, series, data, layout, mode="markers")

    def r_delta_plot(self, data, layout):
        """bar plot with positive values greeen and negative red"""
        raise NotImplementedError

    def r_heatmap_plot(self, data, layout):
        """heatmap"""
        pass

    def r_treemap_plot(self, series, data: list, layout: list):
        """generate a treemap"""
        data_ = pd.DataFrame(self.data)
        print(series)
        data.append(
            go.Treemap(
                data_,
                path=series["x_data"],
                values=[series["y_data"]],
            )
        )
        return data, layout

    def save(self, fig, file_name):
        """write to file"""
        fig.write_image(str(file_name))

    def apply_aesthetics(self, fig):
        """Apply aestetic components"""
        self.set_title(fig)
        self.set_x_label(fig)
        self.set_y_label(fig)
        self.set_size(fig)
