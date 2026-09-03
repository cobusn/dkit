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
from .latex_renderer import LatexRenderer

logger = logging.getLogger("document-builder")

__all__ = [
    "Builder",
    "DocumentCode",
    "DocumentConfiguration",
    "DocumentDefinition",
    "DocumentInfo",
    "ProjectFolderInitializer",
]


class DocumentConfiguration(BaseModel):
    """report config"""
    output: str = "main.pdf"
    renderer: Literal["reportlab", "latex", "docx"]
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

    renderers = {
        "reportlab": RLRenderer,
        "docx": DocxRenderer,
        "latex": LatexRenderer,
    }

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

    def _get_class(self, name):
        """import and retun class"""
        logger.info(f"loading class: {name}")
        l_class = name.split(".")
        class_name = l_class[-1]
        module_name = ".".join(l_class[:-1])
        module_ = import_module(module_name)
        return getattr(module_, class_name)

    @lru_cache
    def _load_code(self):
        """load document code"""
        code = {}
        for k, v in self.definition.code.items():
            class_ = self._get_class(v)
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

    def _get_styler(self, styler):
        """Return styler class specified"""
        if styler == "default":
            styler_class = DefaultStyler
        else:
            styler_class = self._get_class(self.definition.configuration.styler)
            '''
            if not issubclass(styler_class, DefaultStyler):
                styles raise TypeError(f"class {styler_class} is not of the correct type")
            '''
        return styler_class

    def _get_renderer(self, doc):
        renderer = self.renderers[self.definition.configuration.renderer]
        styler = self._get_styler(self.definition.configuration.styler)
        if self.definition.configuration.renderer == "latex":
            return renderer(doc, doc_type=self.definition.configuration.doc_class,
                            styler=styler)
        return renderer(doc, styler=styler)

    def _render(self, doc):
        """Render document"""
        renderer = self._get_renderer(doc)
        if self.definition.configuration.renderer == "latex":
            self._render_latex(renderer)
        else:
            renderer.render(self.definition.configuration.output)

    def _render_latex(self, renderer):
        """render via LatexRenderer, then compile the .tex it wrote to a PDF

        Unlike RLRenderer/DocxRenderer, LatexRenderer.render() only writes
        LaTeX source -- turning that into the file ``configuration.output``
        names needs an external ``pdflatex``, run twice so cross references
        and the table of contents settle. Compiling in a temporary directory
        would break every relative image path a template's plots and
        ``configuration.plot_folder`` rely on, so this runs in place instead,
        next to ``configuration.output`` -- the same convention
        RLRenderer/DocxRenderer already follow -- and only the .aux/.log/.out
        litter pdflatex leaves behind is cleaned up afterwards. The .tex
        itself is kept: it is a normal, inspectable build output, the same
        way :mod:`example_document`'s own scripts treat it.
        """
        if shutil.which("pdflatex") is None:
            raise DKitShellException(
                "pdflatex is not installed. On Ubuntu/Debian: 'apt install "
                "texlive-latex-base texlive-latex-extra'. On RHEL/Fedora: "
                "'dnf install texlive-scheme-basic texlive-collection-latexextra'."
            )
        output = Path(self.definition.configuration.output)
        tex_path = output.with_suffix(".tex")
        renderer.render(str(tex_path))
        for _ in range(2):
            result = subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", tex_path.name],
                cwd=tex_path.parent or ".", capture_output=True, text=True,
            )
            if result.returncode != 0:
                raise DKitApplicationException(
                    f"pdflatex failed to build {output}:\n{result.stdout[-4000:]}"
                )
        for ext in (".aux", ".log", ".out"):
            tex_path.with_suffix(ext).unlink(missing_ok=True)

    def build(self):
        self._render(
            self.build_document()
        )
