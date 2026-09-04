# Copyright (c) 2026 Cobus Nel
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

"""
Report and Document builder.

Manager resources to build a document from templates, code and data.
"""

import importlib.resources
import logging
import shutil
import subprocess
from importlib import import_module
from pathlib import Path
from typing import Any, Literal, Self
from abc import ABC
import yaml
from pydantic import BaseModel
from functools import lru_cache
from ..exceptions import DKitApplicationException, DKitShellException
from . import document as doc
from .rl_renderer import RLRenderer, DefaultStyler
from .docx_renderer import DocxRenderer
from .html_renderer import HtmlRenderer
from .latex_renderer import LatexRenderer

logger = logging.getLogger("document-builder")

__all__ = [
    "Builder",
    "DocumentCode",
    "DocumentConfiguration",
    "DocumentDefinition",
    "DocumentInfo",
    "ProjectFolderInitializer",
    "SimpleDocRenderer",
]

#: every renderer Builder/SimpleDocRenderer know how to produce
RENDERERS = {
    "reportlab": RLRenderer,
    "docx": DocxRenderer,
    "html": HtmlRenderer,
    "latex": LatexRenderer,
}


def get_renderer(document: doc.Document, renderer_name: str,
                 doc_class: str = "article", styler_class=DefaultStyler):
    """instantiate the named renderer, with the kwargs that one expects

    Not every renderer takes the same constructor arguments: only ``latex``
    needs ``doc_type``, and ``html``/``docx`` take no styler at all.  This is
    the one place that distinction is made, so :class:`Builder` and
    :class:`SimpleDocRenderer` cannot drift apart on it.
    """
    renderer_cls = RENDERERS[renderer_name]
    if renderer_name == "latex":
        return renderer_cls(document, doc_type=doc_class, styler=styler_class)
    if renderer_name in ("html", "docx"):
        return renderer_cls(document)
    return renderer_cls(document, styler=styler_class)


def render_to_file(document: doc.Document, renderer_name: str, output: str,
                   doc_class: str = "article", styler_class=DefaultStyler):
    """render ``document`` with the named renderer, writing ``output``

    ``latex`` is the odd one out: :meth:`LatexRenderer.render` only writes
    LaTeX source, so turning that into ``output`` needs an external
    ``pdflatex`` pass, handled by :func:`_compile_latex`.
    """
    renderer = get_renderer(document, renderer_name, doc_class, styler_class)
    if renderer_name == "latex":
        _compile_latex(renderer, output)
    else:
        renderer.render(output)


def _compile_latex(renderer: LatexRenderer, output: str):
    """render via LatexRenderer, then compile the .tex it wrote to a PDF

    Compiling in a temporary directory would break every relative image path
    a template's plots rely on, so this runs in place instead, next to
    ``output`` -- the same convention the other renderers already follow --
    and only the .aux/.log/.out litter pdflatex leaves behind is cleaned up
    afterwards. The .tex itself is kept: it is a normal, inspectable build
    output, the same way :mod:`example_document`'s own scripts treat it.
    """
    if shutil.which("pdflatex") is None:
        raise DKitShellException(
            "pdflatex is not installed. On Ubuntu/Debian: 'apt install "
            "texlive-latex-base texlive-latex-extra'. On RHEL/Fedora: "
            "'dnf install texlive-scheme-basic texlive-collection-latexextra'."
        )
    output_path = Path(output)
    tex_path = output_path.with_suffix(".tex")
    renderer.render(str(tex_path))
    for _ in range(2):
        result = subprocess.run(
            ["pdflatex", "-interaction=nonstopmode", tex_path.name],
            cwd=tex_path.parent or ".", capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise DKitApplicationException(
                f"pdflatex failed to build {output_path}:\n{result.stdout[-4000:]}"
            )
    for ext in (".aux", ".log", ".out"):
        tex_path.with_suffix(ext).unlink(missing_ok=True)


def import_class(name: str):
    """import and return the class named by a dotted path, e.g. ``pkg.mod.Class``"""
    logger.info(f"loading class: {name}")
    l_class = name.split(".")
    class_name = l_class[-1]
    module_name = ".".join(l_class[:-1])
    module_ = import_module(module_name)
    return getattr(module_, class_name)


def resolve_styler(styler: str):
    """the styler named by config: "default", or a dotted class path"""
    if styler == "default":
        return DefaultStyler
    return import_class(styler)


class DocumentConfiguration(BaseModel):
    """report config"""
    output: str = "main.pdf"
    renderer: Literal["reportlab", "latex", "docx", "html"]
    styler: str = "default"
    # latex only: \documentclass{}. Must already be installed and on
    # LaTeX's own search path -- Builder does not ship or locate .cls files.
    doc_class: str = "article"
    plot_folder: str = "/tmp"
    template_folder: str = "templates"


class DocumentInfo(BaseModel):
    """Document properties"""
    author: str = None
    title: str
    sub_title: str | None = None
    title_date: str | None = None
    contact:  str | None


class DocumentDefinition(BaseModel):
    """Report Configuration"""
    version: str = "2.0a"
    info: DocumentInfo
    configuration: DocumentConfiguration
    templates: list[str]
    code: dict[str, str]
    data: dict[str, str]
    variables: dict[str, Any]

    @classmethod
    def generate_sample(cls) -> str:
        """Generate a YAML-formatted sample definition string.

        Returns:
            YAML string suitable for use as a starting template.
        """
        sample = cls(
            info=DocumentInfo(
                title="Document Title",
                sub_title="Document Subtitle",
                author="Author Name",
                contact="author@example.com",
                date="2026-01-01",
            ),
            configuration=DocumentConfiguration(
                renderer="reportlab",
                styler="default",
                plot_folder="plots",
                template_folder="templates",
            ),
            templates=["templates/intro.md"],
            code={"sales": "src.sales.Sales"},
            data={},
            variables={"top_n": 10}
        )
        return yaml.dump(sample.model_dump(), default_flow_style=False, sort_keys=False)

    @classmethod
    def from_file(cls, file_name: str, section=None) -> Self:
        """instantiate from config file

        Args:
            - file_name: name of yaml file
            - section: name of section, use root if not defined

        Returns:
            Builder instance
        """
        with open(file_name, "rt") as infile:
            _config = yaml.safe_load(infile)
        if section is not None:
            return cls(**_config["section"])
        else:
            return cls(**_config)


class DocumentCode(ABC):

    def __init__(self, definition):
        self.definition = definition

    @property
    def data(self):
        return self.definition.data

    @property
    def variables(self):
        return self.definition.variables


class ProjectFolderInitializer:
    """Initialises a project folder structure with scaffold files.

    Args:
        folder: target folder path. Uses current directory when None.
    """

    scaffold = {
        "src": "Code here",
        "images": "Images here",
        "data": "Data here",
        "templates": "Templates here",
    }
    files_scaffold = {
        "src/__init__.py": "src/__init__.py",
        "src/sales.py": "src/sales.py",
        "templates/intro.md": "templates/intro.md",
    }
    config_path = Path("report.yaml")

    def __init__(self, folder: str | Path | None = None):
        self.root = Path(folder) if folder else Path.cwd()

    def _create_subfolder(self, name: str, readme_text: str):
        """Create a subfolder and its README if they do not exist.

        Args:
            name: subfolder name relative to root.
            readme_text: content written to the README file.
        """
        sub = self.root / name
        logger.info("processing folder: %s", sub)
        sub.mkdir(parents=True, exist_ok=True)
        readme = sub / "README"
        if not readme.exists():
            readme.write_text(readme_text + "\n")
            logger.info("created file: %s", readme)

    def _copy_files(self):
        """Copy bundled resource files to the project folder if they do not exist."""
        for src_rel, dest_rel in self.files_scaffold.items():
            dest = self.root / dest_rel
            if not dest.exists():
                src_path = Path(src_rel)
                package = "dkit.resources." + ".".join(src_path.parent.parts)
                src_data = importlib.resources.read_text(package, src_path.name)
                dest.write_text(src_data)
                logger.info("created file: %s", dest)

    def __call__(self):
        """Create scaffold layout::

            <folder>/
                src/README
                images/README
                data/README
                templates/README
                templates/intro.md
                report.yaml

        Existing files and folders are left untouched.
        """
        logger.info("initialising project folder: %s", self.root)

        for name, readme_text in self.scaffold.items():
            self._create_subfolder(name, readme_text)

        config = self.root / self.config_path
        if not config.exists():
            config.write_text(DocumentDefinition.generate_sample())
            logger.info("created file: %s", config)

        self._copy_files()
        logger.info("project folder initialised: %s", self.root)


class Builder:
    """Document Builder"""

    #: kept for backward compatibility; use module-level RENDERERS
    renderers = RENDERERS

    def __init__(self, definition: DocumentDefinition):
        self.definition = definition

    @classmethod
    def from_file(cls, file_name: str, section=None):
        """instantiate from config file

        Args:
            - file_name: name of yaml file
            - section: name of section, use root if not defined

        Returns:
            Builder instance
        """
        return cls(DocumentDefinition.from_file(file_name, section))

    @lru_cache
    def _load_code(self):
        """load document code"""
        code = {}
        for k, v in self.definition.code.items():
            class_ = import_class(v)
            code[k] = class_(self.definition)
        return code

    def build_document(self):
        """build document from templates"""
        _doc = doc.Document(**self.definition.info.model_dump())
        for template_name in self.definition.templates:
            logger.info(f"adding template: {template_name}")
            with open(template_name, "rt") as infile:
                _doc.add_template(infile.read(), **self._load_code())
        return _doc

    def build(self):
        config = self.definition.configuration
        render_to_file(
            self.build_document(), config.renderer, config.output,
            doc_class=config.doc_class, styler_class=resolve_styler(config.styler),
        )


class SimpleDocRenderer:
    """render markdown file(s) straight to a document, no project needed

    The one-shot counterpart to :class:`Builder`: no ``report.yaml``, no
    ``templates``/``code``/``data`` sections, no project folder -- just
    markdown files, a title, and a renderer choice. This is what ``dk build
    doc`` uses.

    args:
        title: document title
        author: document author
        sub_title: document subtitle
        contact: contact details shown on the title page
        title_date: printed date. None uses today's date.
        version: document/report version
        renderer: one of ``RENDERERS`` -- "reportlab", "docx", "html", "latex"
        doc_class: latex only: ``\\documentclass{}`` to use. Must already be
            installed and on LaTeX's own search path.
        styler: "default", or a dotted path to a styler class
    """

    def __init__(self, title: str, author: str | None = None,
                sub_title: str | None = None, contact: str | None = None,
                title_date: str | None = None, version: str | None = None,
                renderer: str = "reportlab", doc_class: str = "article",
                styler: str = "default"):
        self.title = title
        self.author = author
        self.sub_title = sub_title
        self.contact = contact
        self.title_date = title_date
        self.version = version
        self.renderer = renderer
        self.doc_class = doc_class
        self.styler = styler

    def build_from_files(self, output: str, *files: str):
        """render ``files`` (markdown, jinja2-templated) to ``output``"""
        document = doc.Document(
            title=self.title, sub_title=self.sub_title, author=self.author,
            title_date=self.title_date, contact=self.contact, version=self.version,
        )
        document.add_template_files(list(files))
        render_to_file(document, self.renderer, output, self.doc_class,
                       resolve_styler(self.styler))
