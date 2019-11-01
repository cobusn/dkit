import matplotlib.pyplot as plt
from matplotlib import cycler
from matplotlib.ticker import StrMethodFormatter
import numpy as np
import squarify

from . import Backend
from . import ggrammar
from ..exceptions import CkitGrammarException

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
            "geomarea": self.area,
            "geombar": self.bar,
            "geomdelta": self.delta,
            "geomhistogram": self.bar,
            "geomline": self.line,
            "geomscatter": self.scatter,
            "geomtreemap": self.treemap,
            "geomheatmap": self.heatmap,
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
            plt.style.use('ggplot')
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
        # string formatter
        if "float_format" in axes and axes["float_format"]:
            ax.xaxis.set_major_formatter(
                StrMethodFormatter(axes["float_format"])
            )

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

    def set_x_ticks(self, ax):
        """X ticks and labels"""
        width = 0
        x_fields = set([s["x_data"] for s in self.grammar["series"]])
        if len(x_fields) != 1:
            raise CkitGrammarException("Exactly one X Axis field required")
        x_field = list(x_fields)[0]
        x_labels = [row[x_field] for row in self.data]
        x_vals = np.arange(len(x_labels)) + width
        ax.set_xticklabels(x_labels, minor=False)
        ax.set_xticks(x_vals)

        axes = self.grammar["axes"]["0"]
        # rotate labels
        if "rotation" in axes and axes["rotation"]:
            rotate = self.grammar["axes"]["0"]["rotation"]
            for tick in ax.get_xticklabels():
                tick.set_rotation(rotate)

    def x_values(self, series):
        return [r[series["x_data"]] for r in self.data]

    def y_values(self, series):
        return [r[series["y_data"]] for r in self.data]

    def area(self, ax, serie):
        y_vals = self.y_values(serie)
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.plot(x_pos, y_vals, alpha=serie["alpha"])
        ax.fill_between(x_pos, 0, y_vals, alpha=1)
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def bar(self, ax, serie):
        y_vals = self.y_values(serie)
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.bar(
            x_pos,
            y_vals,
            alpha=serie["alpha"]
        )
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def delta(self, ax, serie):
        """bar plot with positive values greeen and negative red"""
        y_vals = self.y_values(serie)
        y_pos = [i if i > 0 else 0 for i in y_vals]
        y_neg = [i if i < 0 else 0 for i in y_vals]
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.bar(x_pos, y_pos, alpha=serie["alpha"], color=self.green_color)
        ax.bar(x_pos, y_neg, alpha=serie["alpha"], color=self.red_color)
        ax.tick_params(axis="x", which="both", length=0)
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def heatmap(self, ax, serie):
        """heatmap"""
        data = list(self.data)
        x_values = set(self.x_values(serie))
        y_values = set(self.y_values(serie))
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

    def line(self, ax, serie):
        y_vals = self.y_values(serie)
        x_pos = [i for i, _ in enumerate(y_vals)]
        ax.plot(
            x_pos,
            y_vals,
            alpha=serie["alpha"]
        )
        self.set_x_ticks(ax)
        self.set_labels(ax)

    def scatter(self, ax, series):
        x_vals = self.x_values(series)
        y_vals = self.y_values(series)
        ax.scatter(
            x_vals,
            y_vals,
            alpha=series["alpha"]
        )

    def treemap(self, ax, serie):
        # format values
        if serie["str_format"]:
            fmt = serie["str_format"]
            values = [fmt.format(i) for i in self.y_values(serie)]
        else:
            values = self.y_values(serie)

        squarify.plot(
            sizes=self.y_values(serie),
            label=self.x_values(serie),
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
