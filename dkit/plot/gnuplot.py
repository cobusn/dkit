import subprocess

import jinja2

from .base import Backend
from . import ggrammar
from ..utilities import file_helper as fh
#
# dumb enhanced ansi256"
#
base_template = """
# data
{{ util.get_data() }}
\
# terminal
set term {{ util.get_terminal() }}
\
{# - output --------------------------------------------------------------- #}
{% if file_name -%}
set output "{{ file_name }}"
{% endif -%}
\
{# -- title  --------------------------------------------------------------- #}
{% if plot.title %}
set title "{{ plot.title }}"
{% endif %}
\
{# -- y-axis --------------------------------------------------------------- #}
{% if plot.axes[0] %}
set ylabel "{{ plot.axes[0].title }}"
set yrange [{{ util.get_axis_range() }}]
{% endif %}
\
{# -- x-axis --------------------------------------------------------------- #}
{% if plot.axes[1] %}
set xlabel "{{ plot.axes[1].title }}"
{% endif %}
\
set xtics autofreq rotate by -45

{# -- aesthetic section ---------------------------------------------------- #}
{% if plot.series[0].isbox %}
set boxwidth {{ plot.aes.box_width }} relative
{% if plot.aes.stacked %}
set style histogram rowstacked
{% else %}
set style histogram clustered
{% endif %}
{% endif %}
\
{# -- plot section --------------------------------------------------------- #}
# Plot section
plot $DATA \
{% for series in plot.series  %}
{%- if loop.first %}
{{ util.get_series(series) }}
{%- else %}
, "" {{ util.get_series(series) }}
{%- endif %}
{% endfor %}

"""

template_dict = {
    "base.plot": base_template,
}


class PlotUtilities():

    def __init__(self, renderer, grammar):
        self.renderer = renderer
        self.plot = grammar
        self.data = grammar["data"]

    @staticmethod
    def quote(string):
        """
        quote a string
        """
        return '"' + string + '"'

    def get_columns(self):
        """
        retrieve column names from a plot
        """
        y_axis = set([s["y_data"] for s in self.plot["series"]])
        x_axis = set([s["x_data"] for s in self.plot["series"] if s["x_data"] is not None])
        return y_axis.union(x_axis)

    def get_axis_range(self, y_axis=0):
        """Generate axis range specifier"""
        axis = self.plot["axes"][y_axis]
        rmin = "*" if axis["min_val"] is None else axis["min_val"]
        rmax = "*" if axis["max_val"] is None else axis["max_val"]
        return "{}:{}".format(rmin, rmax)

    def get_terminal(self):
        """
        terminal with appropriate settings
        """
        aes = ggrammar.Aesthetic.from_dict(self.plot["aes"])
        term = "{}".format(self.renderer.terminal)
        if self.renderer.terminal in ["pdf"]:
            if (aes.width is not None) and (aes.height is not None):
                term += " size {width}{unit},{height}{unit}".format(**aes.__dict__)
            term += ' font "{font},{font_size}"'.format(**aes.__dict__)

        # PNG, JPG, GIF
        elif self.renderer.terminal in ["png", "jpg", "gif"]:
            if (aes.width is not None) and (aes.height is not None):
                term += " size {pixel_width},{pixel_height}".format(
                    pixel_width=aes.pixel_width,
                    pixel_height=aes.pixel_height,
                )
            term += ' font "{font},{font_size}"'.format(**aes.__dict__)

        # SVG
        elif self.renderer.terminal in ["svg"]:
            if (aes.width is not None) and (aes.height is not None):
                term += " size {pixel_width},{pixel_height}".format(
                    pixel_width=aes.pixel_width,
                    pixel_height=aes.pixel_height,
                )
            term += ' enhanced'
            term += ' font "{font}, {font_size}"'.format(**aes.__dict__)

        return term

    def get_series(self, series):
        kind = series["~>"]
        plot_type = {
            "geombar": "histogram",
            "geomhistogram": "histogram",
            "geomline": "lines",
            "geomarea": "filledcurves x1",
            "geomimpulse": "impulses",
            "geomscatter": "points",
        }
        # Data
        if series["x_data"] is None:
            data_section = 'using "{}" '.format(series.y_data)
        else:
            if kind == "GeomScatter":
                data_section = f'using "{series.x_data}":"{series.y_data}" '
            else:
                data_section = 'using "{}":xtic(stringcolumn("{}")) '.format(
                    series["y_data"], series["x_data"]
                )

        # Style
        style_section = "with {} ".format(plot_type[kind])

        # Color
        if series["color"] is None:
            color_section = ""
        else:
            color_section = 'linecolor rgb "{}" '.format(series["color"])

        # Fill & Alpha
        if series["primitive_type"] in ["box"]:
            if series.alpha is None:
                alpha_section = ""
            else:
                alpha_section = "fill transparent solid {} ".format(series.alpha)
        else:
            alpha_section = ""

        # Title
        if series["title"] is None:
            title_section = "notitle "
        else:
            title_section = 'title "{}"'.format(series["title"])

        return data_section + style_section + color_section + alpha_section + title_section

    def get_data(self, sep="\t"):
        """
        retrieve data for plot
        """
        cols = self.get_columns()
        retval = "$DATA << EOD\n"
        retval += sep.join(self.get_columns()) + "\n"
        for row in self.data:
            col_values = [row[k] for k in cols]
            for i, v in enumerate(col_values):
                if isinstance(v, str):
                    col_values[i] = '"' + v + '"'
                elif isinstance(v, int):
                    col_values[i] = f"{col_values[i]:d}"
                else:
                    col_values[i] = f"{col_values[i]:f}"
            retval += sep.join([str(i) for i in col_values]) + "\n"
        retval += "EOD\n"
        return retval


class BackendGnuPlot(Backend):
    """
    Render plot using GnuPlot

    args:
        * terminal: svg, png, jpg, dumb
    """
    def __init__(self, terminal="pdf", styler=None, enhanced: bool = True):
        super().__init__(terminal, styler)
        self.enhanced = enhanced

    @property
    def width(self):
        return self.aes.width

    @property
    def height(self):
        return self.aes.height

    def _get_template(self, name):
        return template_dict[name]

    def render_str(self, grammar):
        """
        Helper to render plot to unicode string

        temporarily set terminal to dumb and write
        to a tempfile
        """
        super().render(grammar, None)
        self.aes = ggrammar.Aesthetic.from_dict(self.grammar["aes"])

        save_terminal = self.terminal
        width = self.width if self.width else 78
        height = self.height if self.height else 24
        if self.enhanced:
            self.terminal = f"dumb size {width} {height} enhanced ansi256"
        else:
            self.terminal = "dumb nofeed noenhanced"
        file_name = fh.temp_filename()
        self.render(grammar, file_name, None)
        self.terminal = save_terminal
        with open(file_name) as infile:
            rv = infile.read()
        return rv

    def render(self, grammar, file_name: str, script_name: str = None):
        """
        generate ouput

        args:
            * file_name: output file name
            * script_name: gnuplot script file name, will use tempfile if None
        """
        super().render(grammar, file_name)
        self.aes = ggrammar.Aesthetic.from_dict(self.grammar["aes"])

        def write_and_run(plot, script_name, file_name):
            with open(script_name, "wt") as handle:
                handle.write(self._render_source(plot, file_name))
                handle.flush()
                params = ["gnuplot", source_file_name]
                result = subprocess.run(
                    params,
                    stdout=subprocess.PIPE
                )
                return result.returncode

        if script_name is None:
            source_file_name = fh.temp_filename()
        else:
            source_file_name = script_name
        write_and_run(self.grammar, source_file_name, file_name)

    def _render_source(self, plot, file_name):
        env = jinja2.Environment(
            loader=jinja2.FunctionLoader(self._get_template),
            trim_blocks=True,
            lstrip_blocks=True
        )

        t = env.get_template("base.plot")
        return t.render(
            plot=plot,
            util=PlotUtilities(self, plot),
            file_name=file_name,
        )
