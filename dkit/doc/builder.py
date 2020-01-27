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

import functools
import pathlib
import subprocess
from abc import ABC
from importlib import import_module
from pathlib import Path

import jinja2
import mistune
import yaml
from IPython import get_ipython
from IPython.display import HTML
from tabulate import tabulate
from string import Template
from . import md_to_json, latex_renderer, schemas
from ..etl import source
from ..plot import matplotlib
from ..utilities import log_helper as lh
from ..data import json_utils as ju

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def is_in_notebook():
    """
    return True if code is run in a Jupyter notebook
    """
    if get_ipython():
        return True
    else:
        return False


class ReportContent(ABC):

    def __init__(self, parent):
        self.parent = parent
        self.configure()

    @property
    def data(self):
        return self.parent.data

    @property
    def variables(self):
        return self.parent.variables

    def configure(self):
        """
        Hook to perform initialisation without having to boilerplate the
        constructor
        """
        pass


def jsonise(fn) -> str:
    """Format output of function as json.

    Args:
        - fn: class with as_dict function

    Returns:
        - string
    """
    encoder = ju.JsonSerializer(
        ju.DateTimeCodec(),
        ju.DateCodec(),
        ju.Decimal2FloatCodec()
    )
    j = encoder.dumps(fn.as_dict())
    return f"```jsoninclude\n{j}\n```\n"


def is_plot(func):
    """Decorator that designate output as a plot"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if is_in_notebook():
            the_dict = func(*args, **kwargs).as_dict()
            renderer = matplotlib.MPLBackend(
                the_dict
            )
            return renderer.render(the_dict["filename"])
        else:
            return jsonise(func(*args, **kwargs))

    return wrapper


def is_table(func):
    """Decorator that designate report output as table"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if is_in_notebook():
            the_dict = func(*args, **kwargs).as_dict()
            return HTML(
                tabulate.tabulate(
                    the_dict["data"],
                    headers="keys",
                    tablefmt="html"
                )
            )
        else:
            return jsonise(func(*args, **kwargs))

    return wrapper


class LatexRunner(object):
    """
    calls pdflatex to generate a pdf file from latex source
    """
    def __init__(self, tex_filename, command="pdflatex", output_folder="."):
        self.tex_filename = tex_filename
        self.command = command
        self.output_folder = output_folder.strip()

    def run(self):
        """
        call latex command to build specified file
        """
        cmd = [self.command, '-interaction', 'nonstopmode',
               f"-output-directory={self.output_folder}", self.tex_filename]

        proc = subprocess.Popen(
            cmd,
            # stdout=subprocess.PIPE,
            # stderr=subprocess.PIPE
        )

        out, err = proc.communicate()
        retcode = proc.returncode

        if not retcode == 0:
            raise ValueError('Error {} executing command: {}'.format(retcode, ' '.join(cmd)))

    def clean(self):
        """
        clean all associated tex logiles etc.

        will silently catch any FileNotFoundError
        """
        stem = (Path.cwd() / self.tex_filename).stem
        clean_extensions = ["log", "aux", "idx", "out", "snm", "toc", "nav", "vrb"]
        _path = Path.cwd() / self.output_folder
        for ext in clean_extensions:
            path = _path / f"{stem}.{ext}"
            try:
                path.unlink()
            except FileNotFoundError:
                pass


class ReportBuilder(object):
    """
    Build report from configuration

    use a from_ .. method to initialize
    """
    def __init__(self, definition, logger=None):
        self.definition = definition
        self.data = {}
        self.variables = {}
        self.code = {
            "len": len,
            # "currency": lambda x: "R{:,.0f}".format(x)
            "currency": self.__fmt_currency,
            "variables": self.variables,
        }
        self.documents = {}
        self.presentations = {}
        self.style_sheet = {}
        self.logger = logger if logger else lh.stderr_logger(self.__class__.__name__)
        self.logger.info("Validating report definition")

        # validate defition
        validator = schemas.SchemaValidator(schemas.report_schema, self.logger)
        validator(self.definition)

    def __fmt_currency(self, the_str):
        return "R{:,.0f}".format(the_str)

    @property
    def plot_folder(self):
        return self.definition["configuration"]["plot_folder"]

    def load_data(self):
        """
        load all datasets
        """
        for k, v in self.definition["data"].items():
            filename = Template(v).safe_substitute(self.variables)
            self.logger.info(f"loading data file: {filename}")
            with source.load(filename) as iter_in:
                self.data[k] = list(iter_in)

    def load_code(self):
        """
        load all code
        """
        for k, v in self.definition["code"].items():
            self.logger.info(f"loading class: {v}")
            l_class = v.split(".")
            class_name = l_class[-1]
            module_name = ".".join(l_class[:-1])
            module_ = import_module(module_name)
            class_ = getattr(module_, class_name)
            self.code[k] = class_(self)

    def load_variables(self):
        """
        Load report variables
        """
        if "variables" in self.definition:
            self.variables.update(self.definition["variables"])

    def load_stylesheets(self):
        """
        load stylesheets
        """
        for sheet in self.definition["styles"]:
            self.logger.info(f"loading stylesheet: {sheet}")
            with open(sheet, "rt") as infile:
                self.style_sheet.update(yaml.load(infile, Loader=Loader))

    def load_documents(self):
        """
        load all templates
        """
        for k, v in self.definition["documents"].items():
            self.logger.info(f"loading document template: {v}")
            template_name = pathlib.Path.cwd() / v
            with open(template_name) as tpl_data:
                self.documents[k] = jinja2.Template(tpl_data.read())

    def load_presentations(self):
        """
        load all slide templates
        """
        for k, v in self.definition["presentations"].items():
            self.logger.info(f"loading presentation template: {v}")
            template_name = pathlib.Path.cwd() / v
            with open(template_name) as tpl_data:
                self.presentations[k] = jinja2.Template(tpl_data.read())

    @classmethod
    def from_file(cls, file_name):
        """
        constructor. file is a yaml file
        """
        with open(file_name) as infile:
            j = yaml.load(infile, Loader=Loader)
        return cls(j)

    def render(self, renderer, documents):
        """
        render report

        arguments:
            - renderer: object that implement renderer interface
                        (e.g. LatexReport)
            - documents: dictionary of documents
        """
        for _name, _template in documents.items():
            # create latex
            self.logger.info(f"rendering template to {_name}")
            rendered = _template.render(**self.code)

            # create json cannonical format
            md = mistune.Markdown(renderer=md_to_json.JSONRenderer())
            dict_ = md(rendered)

            # render to endpoint
            r = renderer(dict_, style_sheet=self.style_sheet, plot_folder=self.plot_folder)
            with open(_name, "wt") as out_file:
                out_file.write("".join(r))

    def build_output(self):
        for _name in self.definition["latex"]:
            runner = LatexRunner(_name, output_folder="output")
            runner.run()
            runner.clean()

    def run(self, report_type="latex"):
        self.load_variables()
        self.load_data()
        self.load_code()
        self.load_stylesheets()
        self.load_documents()
        self.load_presentations()

        if report_type == "latex":
            self.render(latex_renderer.LatexDocRenderer, self.documents)
            self.render(latex_renderer.LatexBeamerRenderer, self.presentations)
            self.build_output()
