import matplotlib.pyplot as plt
from matplotlib import cycler
from matplotlib.ticker import StrMethodFormatter, PercentFormatter
from matplotlib.offsetbox import AnchoredText
import numpy as np
import squarify
from itertools import accumulate
from . import Backend
from . import ggrammar
from ..data.filters import ExpressionFilter
# from ..exceptions import CkitGrammarException

import pprint
pp = pprint.PrettyPrinter(indent=4)


def to_inch(size):
    """
    confert size to pixels using stored dpi
    """
    return 0.393701 * size


class MPLBackend(Backend):
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

    def initialise(self):
        """
        Initialise Plot and Aesthetic

        returns fig, axes
        """
        fig, axes = plt.subplots()
        return (fig, axes)

    def _apply_style(self):
        """Apply settings specified in stylesheet"""
        style = self.style_sheet["plot"]
        if "theme" in style:
            plt.style.use(style["theme"])
        else:
            plt.style.use("default")
        if "width" in style and "height" in style:
            plt.rc("figure", figsize=(
                to_inch(style["width"]),
                to_inch(style["height"])
            ))
        if "axes" in style:
            axes_ = style["axes"]
            if "titlesize" in axes_:
                plt.rc("axes", titlesize=axes_["titlesize"])
            if "labelsize" in axes_:
                plt.rc("axes", labelsize=axes_["labelsize"])
            if "ticklabelsize" in axes_:
                plt.rc("xtick", labelsize=axes_["ticklabelsize"])
                plt.rc("ytick", labelsize=axes_["ticklabelsize"])
            if "linewidth" in axes_:
                plt.rc("axes", linewidth=axes_["linewidth"])

        if "colors" in style:
            colors = style["colors"]
            if "palette" in colors:
                colors = cycler("color", colors["palette"])
                plt.rc("axes", prop_cycle=colors)
            if "red" in colors:
                self.red_color = colors["red"]
            if "green" in colors:
                self.green_color = colors["green"]

    def set_title(self, ax):
        ax.set_title(self.grammar["title"])

    def get_z_label(self, ax):
        axes = self.grammar["axes"]["2"]
        if "title" in axes:
            return axes["title"]
        else:
            return ""

    def set_x_label(self, ax):
        axes = self.grammar["axes"]["0"]
        if "title" in axes:
            ax.set_xlabel(axes["title"])

    def set_y_label(self, ax):
        axes = self.grammar["axes"]["1"]
        if "title" in axes:
            ax.set_ylabel(self.grammar["axes"]['1']["title"])
        # string formatter
        if "float_format" in axes and axes["float_format"]:
            ax.yaxis.set_major_formatter(
                StrMethodFormatter(axes["float_format"])
            )

    def set_labels(self, ax):
        self.set_x_label(ax)
        self.set_y_label(ax)

    def set_x_ticks(self, ax, hist=False):
        """X ticks and labels"""
        width = 0
        # x_fields = set([s["x_data"] for s in self.grammar["series"]])
        # if len(x_fields) != 1:
        #     raise CkitGrammarException("Exactly one X Axis field required")
        series_0 = self.grammar["series"][0]
        axes = self.grammar["axes"]["0"]
        x_labels = self.get_x_values(series_0)
        x_vals = np.arange(len(x_labels)) + width

        # apply float format
        if "float_format" in axes and axes["float_format"]:
            fmt = axes["float_format"]
            x_labels = [fmt.format(i) for i in x_labels]

        if "time_format" in axes and axes["time_format"]:
            fmt = axes["time_format"]
            x_labels = [i.strftime(fmt) for i in x_labels]

        if hist:  # remove leftmost label for histograms
            x_labels[0] = ""

        # so not draw if suppress is specified
        if ("defeat" not in axes) or ("defeat" in axes and not axes["defeat"]):
            ax.set_xticklabels(x_labels, minor=False)
            ax.set_xticks(x_vals)

            # rotate labels
            if "rotation" in axes and axes["rotation"]:
                rotate = self.grammar["axes"]["0"]["rotation"]
                for tick in ax.get_xticklabels():
                    tick.set_rotation(rotate)

    def get_color(self, series):
        if "color" in series and series["color"]:
            return series["color"]
        else:
            return None

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

    def get_line_style(self, series):
        if "line_style" in series:
            return series["line_style"]
        else:
            return None

    def anchored_text(self, ax, serie):
        """Add anchored text"""
        at = AnchoredText(
            serie["text"],
            loc=serie["location"],
            prop=dict(
                alpha=serie["alpha"],
                size=serie["size"]
            ),
            frameon=False
        )
        # at.patch.set_boxstyle("round,pad=0.,rounding_size=0.2")
        ax.add_artist(at)

    def render(self, file_name):
        self.fig, ax = self.initialise()
        for serie in self.grammar["series"]:
            renderer = self.render_map[serie["~>"]]
            renderer(ax, serie)
        self.apply_aesthetics()
        self.set_title(ax)
        self.save(self.fig, file_name)

        # release memory used by plot
        plt.close(self.fig)
        return self.fig

    def r_cumulative_series(self, ax, serie):
        # ax2.plot(df.index, "], color="C1", marker="D", ms=7)
        total = sum(self.get_y_values(serie))
        y_vals = list((i/total)*100 for i in accumulate(self.get_y_values(serie)))
        x_pos = [i for i, _ in enumerate(y_vals)]

        ax2 = ax.twinx()
        ax2.yaxis.set_major_formatter(PercentFormatter())
        ax2.plot(
            x_pos,
            y_vals,
            alpha=serie["alpha"],
            color="C1",
            marker="None",
            linewidth=0.5
        )
        ax2.tick_params(axis="y", colors="C1")
        ax2.grid(False)
        ax2.set_ylabel("Cumulative")

        self.set_x_ticks(ax)
        self.set_labels(ax)

    def r_hline(self, ax, serie):
        """draw horizontal line"""
        ax.axhline(serie["y"], color=serie["color"], linewidth=serie["line_width"],
                   linestyle=serie["line_style"], alpha=serie["alpha"])

    def r_vline(self, ax, serie):
        """draw vertical line"""
        ax.axvline(serie["x"], color=serie["color"], linewidth=serie["line_width"],
                   linestyle=serie["line_style"], alpha=serie["alpha"])

    def r_fill_plot(self, ax, serie):
        """two lines with area between filled"""
        y_upper = self.get_y_values(serie, "y_upper")
        y_lower = self.get_y_values(serie, "y_lower")
        x_pos = [i for i, _ in enumerate(y_upper)]
        if "color" in serie and serie["color"]:
            color = serie["color"]
        else:
            color = None
        ax.plot(x_pos, y_upper, alpha=serie["line_alpha"], color=color, linewidth=0.5)
        ax.plot(x_pos, y_lower, alpha=serie["line_alpha"], color=color, linewidth=0.5)
        ax.fill_between(x_pos, y_lower, y_upper, color=color,
                        alpha=serie["fill_alpha"])
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def r_area_plot(self, ax, serie):
        y_vals = self.get_y_values(serie)
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.plot(x_pos, y_vals, alpha=serie["alpha"])
        ax.fill_between(x_pos, 0, y_vals, alpha=1)
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def r_hist_plot(self, ax, serie):
        self.r_bar_plot(ax, serie, hist=True)

    def r_bar_plot(self, ax, serie, hist=False):
        y_vals = self.get_y_values(serie)
        if hist:
            x_pos = [i + 0.5 for i, _ in enumerate(y_vals)]
        else:
            x_pos = [i for i, _ in enumerate(y_vals)]
        ax.bar(
            x_pos,
            y_vals,
            width=1,
            alpha=serie["alpha"]
        )
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax, hist)
        self.set_labels(ax)

    def r_delta_plot(self, ax, serie):
        """bar plot with positive values greeen and negative red"""
        y_vals = self.get_y_values(serie)
        y_pos = [i if i > 0 else 0 for i in y_vals]
        y_neg = [i if i < 0 else 0 for i in y_vals]
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.bar(x_pos, y_pos, alpha=serie["alpha"], color=self.green_color)
        ax.bar(x_pos, y_neg, alpha=serie["alpha"], color=self.red_color)
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def r_heatmap_plot(self, ax, serie):
        """heatmap"""
        data = list(self.data)
        x_values = set(self.get_x_values(serie))
        y_values = set(self.get_y_values(serie))
        x_dim = len(x_values)
        y_dim = len(y_values)

        x_map = {k: i for i, k in enumerate(x_values)}
        y_map = {k: i for i, k in enumerate(y_values)}

        # create heatmap
        heatmap = np.empty((y_dim, x_dim))
        heatmap[:] = np.nan

        for row in data:
            x = x_map[row[serie["x_data"]]]
            y = y_map[row[serie["y_data"]]]
            z = row[serie["z_data"]]
            heatmap[y, x] = z

        im = ax.imshow(heatmap, interpolation='nearest', origin='lower')
        cbar = self.fig.colorbar(ax=ax, mappable=im, orientation='vertical')
        cbar.set_label(self.get_z_label(ax))
        self.set_labels(ax)

    def r_line_plot(self, ax, serie):
        """render line series"""
        y_vals = self.get_y_values(serie)
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.plot(
            x_pos,
            y_vals,
            color=serie["color"],
            alpha=serie["alpha"],
            linestyle=self.get_line_style(serie)
        )
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def r_scatter_plot(self, ax, series):
        """render scatter plot"""
        ax.scatter(
            self.get_x_values(series),
            self.get_y_values(series),
            color=self.get_color(series),
            alpha=series["alpha"]
        )
        self.set_labels(ax)

    def r_treemap_plot(self, ax, serie):
        # format values
        if serie["str_format"]:
            fmt = serie["str_format"]
            values = [fmt.format(i) for i in self.get_y_values(serie)]
        else:
            values = self.get_y_values(serie)

        squarify.plot(
            sizes=self.get_y_values(serie),
            label=self.get_x_values(serie),
            value=values,
            alpha=serie["alpha"],
            ax=ax
        )
        ax.axis("off")

    def save(self, fig, file_name):
        fig.savefig(file_name)

    def apply_aesthetics(self):
        # size
        if self.aes.height:
            self.fig.set_figheight(self.aes.inch_height)
        if self.aes.width:
            self.fig.set_figwidth(self.aes.inch_width)
        self.fig.tight_layout()
        self.fig.subplots_adjust(top=0.85)
