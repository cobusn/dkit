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
import logging
import pathlib
from importlib import import_module
from pathlib import Path
from string import Template
from abc import ABC
import inflect
import jinja2
import mistune
import yaml
from IPython import get_ipython
from IPython.display import HTML
from tabulate import tabulate

from . import json_renderer, latex_renderer, schemas
from .. import __version__
from ..data import json_utils as ju
from ..etl import source
from ..plot import matplotlib
from .document import Document, __report_version__
from .lorem import Lorem
from .latex import LatexRunner

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


logger = logging.getLogger(__name__)


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
            return renderer.render("plots/" + the_dict["filename"])
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
                tabulate(
                    the_dict["data"],
                    headers="keys",
                    tablefmt="html"
                )
            )
        else:
            return jsonise(func(*args, **kwargs))

    return wrapper


class BuilderProxy(object):
    """load a subset of report configuration (data and varialbes)

    Cannot render a report but useful for use with Jupyter notebooks
    """

    def __init__(self, definition):
        self.definition = definition
        self.configuration = {}
        self.data = {}
        self.variables = {}
        logger.info("Validating report definition")
        # validate defition
        validator = schemas.SchemaValidator(schemas.report_schema)
        validator(self.definition)
        self.load_variables()
        self.load_data()

    @property
    def plot_folder(self):
        if "plot_folder" in self.definition["configuration"]:
            return self.definition["configuration"]["plot_folder"]
        else:
            return "plots"

    def load_data(self):
        """
        load all datasets
        """
        for k, v in self.definition["data"].items():
            filename = Template(v).safe_substitute(self.variables)
            logger.info(f"loading data file: {filename}")
            with source.load(filename) as iter_in:
                self.data[k] = list(iter_in)

    def load_variables(self):
        """
        Load report variables
        """
        if "variables" in self.definition:
            self.variables.update(self.definition["variables"])
        self.configuration.update(self.definition["configuration"])

    @classmethod
    def from_file(cls, file_name):
        """
        constructor. file is a yaml file
        """
        with open(file_name) as infile:
            definition = yaml.load(infile, Loader=Loader)
        if definition["configuration"]["builder"] == "reportlab":
            return ReportLabBuilder(definition)
        elif definition["configuration"]["builder"] == "latex":
            return LatexReportBuilder(definition)


class ReportBuilder(BuilderProxy):
    """
    Build report from configuration

    use a from_ .. method to initialize
    """
    def __init__(self, definition):
        super().__init__(definition)
        self.code = {
            "len": len,
            "currency": self.__fmt_currency,
            "variables": self.variables,
            "data": self.data,
            "inflect": inflect.engine(),
            "lorem": Lorem(),
        }
        self.documents = {}
        self.presentations = {}
        self.style_sheet = {}
        self.environment = None  # Jinja2 environment
        self.load_code()
        self.load_stylesheets()
        self.load_documents()
        self.load_presentations()

    @property
    def builder_type(self):
        return self.definition["configuration"]["builder"]

    def __fmt_currency(self, the_str):
        return "R{:,.0f}".format(the_str)

    def load_code(self):
        """
        load all code
        """
        for k, v in self.definition["code"].items():
            logger.info(f"loading class: {v}")
            l_class = v.split(".")
            class_name = l_class[-1]
            module_name = ".".join(l_class[:-1])
            module_ = import_module(module_name)
            class_ = getattr(module_, class_name)
            self.code[k] = class_(self)

    def load_stylesheets(self):
        """
        load stylesheets
        """
        for sheet in self.definition["styles"]:
            logger.info(f"loading stylesheet: {sheet}")
            with open(sheet, "rt") as infile:
                self.style_sheet.update(yaml.load(infile, Loader=Loader))

    def load_documents(self):
        """
        load all templates
        """
        loader_path = Path().cwd() / self.configuration["template_folder"]
        loader = jinja2.FileSystemLoader(str(loader_path))
        self.environment = jinja2.Environment(loader=loader)
        for template_name in self.definition["documents"]:
            doc_name = template_name.replace(".md", "")
            logger.info(f"loading document template: {template_name}")
            self.documents[doc_name] = self.environment.get_template(template_name)

    def load_presentations(self):
        """
        load all slide templates
        """
        for k, v in self.definition["presentations"].items():
            logger.info(f"loading presentation template: {v}")
            template_name = pathlib.Path.cwd() / v
            with open(template_name) as tpl_data:
                self.presentations[k] = jinja2.Template(tpl_data.read())

    def run(self):
        raise NotImplementedError

    def render_templates(self):
        raise NotImplementedError


class ReportLabBuilder(ReportBuilder):

    def run(self):
        """build report using reportlab module"""
        from dkit.doc.reportlab_renderer import ReportLabRenderer
        #
        # add logic to instantiate styler etc..
        #
        b = ReportLabRenderer()
        doc = self.render_templates(self.documents)
        b.run("reportlab_render.pdf", doc)

    def render_templates(self, documents):
        """
        render report

        arguments:
            - renderer: object that implement renderer interface
                        (e.g. LatexReport)
            - documents: dictionary of documents
        """
        class __ProxyDocument(Document):
            """
            modified Document class to support elements that have already
            been translated to dict format
            """
            def as_dict(self):
                return {
                    "library_version": __version__,
                    "report_version": __report_version__,
                    "title": self.title,
                    "sub_title": self.sub_title,
                    "author": self.author,
                    "contact": self.contact,
                    "email": self.email,
                    "date": self.date,
                    "data": self.store,
                    "elements": self.elements
                }

        content = []
        for _name, _template in documents.items():
            logger.info(f"rendering template to {_name}")
            rendered = _template.render(**self.code)

            # create json cannonical format
            md = mistune.Markdown(renderer=json_renderer.JSONRenderer())
            content += md(rendered)
        retval = __ProxyDocument(**self.definition["document"])
        retval.elements = content
        return retval


class LatexReportBuilder(ReportBuilder):

    """ReportBuilder specialized for building Latex projects"""
    def run(self, _name="tex/main.tex"):
        self.render_templates(latex_renderer.LatexDocRenderer, self.documents)
        self.render_templates(latex_renderer.LatexBeamerRenderer, self.presentations)
        runner = LatexRunner(_name, output_folder="output")
        runner.run()
        runner.clean()

    def render_templates(self, renderer, documents):
        """
        render report

        arguments:
            - renderer: object that implement renderer interface
                        (e.g. LatexReport)
            - documents: dictionary of documents
        """
        for _name, _template in documents.items():
            # create latex
            logger.info(f"rendering template to {_name}")
            rendered = _template.render(**self.code)

            # create json cannonical format
            md = mistune.Markdown(renderer=json_renderer.JSONRenderer())
            dict_ = md(rendered)

            # render to endpoint
            r = renderer(dict_, style_sheet=self.style_sheet, plot_folder=self.plot_folder)
            with open(f"output/{_name}.tex", "wt") as out_file:
                out_file.write("".join(r))
