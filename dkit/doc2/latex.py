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
LaTeX generation library for doc2.

Provides building-block classes that map to LaTeX constructs. The
top-level Document class assembles them into a complete .tex file
via str(doc).
"""

import re

import tabulate as tabulate_mod

from ..data.helpers import scale

__all__ = [
    "Bold",
    "BlockQuote",
    "Chapter",
    "Comment",
    "Document",
    "Emph",
    "Enumerate",
    "Heading",
    "Href",
    "Image",
    "Inline",
    "Item",
    "Itemize",
    "Latex",
    "LineBreak",
    "Listing",
    "LongTable",
    "NewPage",
    "Paragraph",
    "Section",
    "SimpleTable",
    "SubSection",
    "SubSubSection",
    "Url",
    "Verb",
    "Verbatim",
    "encode",
    "TexError",
]

VALID_DOC_TYPES = ["article", "report", "book", "letter", "scrartcl"]
VALID_FONT_SIZES = [10, 11, 12]
VALID_PAPER_SIZES = [
    "a4paper", "letterpaper", "a5paper", "b5paper",
    "legalpaper", "executivepaper",
]
VALID_ALIGNMENTS = ["center", "left", "right"]

TEX_MESSAGES = {
    "ERR_INVALID_DOCTYPE": (
        "invalid document type; must be a LaTeX class name (e.g. one of [%s], "
        "or a custom .cls already installed and on LaTeX's own search path)"
        % ", ".join(VALID_DOC_TYPES)
    ),
    "ERR_INVALID_FONT_SIZE": (
        "invalid font size; must be one of [%s]"
        % ", ".join(str(i) for i in VALID_FONT_SIZES)
    ),
    "ERR_INVALID_PAPER_SIZE": (
        "invalid paper size; must be one of [%s]" % ", ".join(VALID_PAPER_SIZES)
    ),
    "ERR_INVALID_ALIGNMENT": (
        "invalid alignment; must be one of [%s]" % ", ".join(VALID_ALIGNMENTS)
    ),
    "ERR_INT_REQUIRED": "integer parameter required",
    "ERR_INVALID_LEVEL": "invalid heading level specified",
}

SPECIAL_CHARACTERS = {
    "#": r"\#",
    "&": r"\&",
    "%": r"\%",
    "£": r"\pounds",
    "€": r"\euro{}",
    "$": r"\$",
    "_": r"\_",
    "~": r"\textasciitilde{}",
    "^": r"\textasciicircum{}",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TexError(Exception):
    """Raised for invalid LaTeX construct parameters."""


#: $...$ or $$...$$ math, left untouched by encode(). mistune has no math
#: plugin wired up, so a math span arrives here as plain text already meant
#: to be raw LaTeX -- test/input_files/reference.tex, v1's own golden output,
#: shows $...$/$$...$$ passing through verbatim, not escaped.
_MATH_RE = re.compile(r"\$\$.*?\$\$|\$[^$\n]+\$", re.DOTALL)


def encode(data: str) -> str:
    """Escape LaTeX special characters in data, outside of math spans.

    Args:
        data: plain-text string to escape.

    Returns:
        String with LaTeX special characters replaced, except inside any
        $...$/$$...$$ math span, which is passed through verbatim.
    """
    parts = []
    pos = 0
    for m in _MATH_RE.finditer(data):
        parts.append(_escape(data[pos:m.start()]))
        parts.append(m.group())
        pos = m.end()
    parts.append(_escape(data[pos:]))
    return "".join(parts)


def _escape(data: str) -> str:
    """Escape LaTeX special characters in data, with no math-span awareness."""
    for k, v in SPECIAL_CHARACTERS.items():
        if k in data and v not in data:
            data = data.replace(k, v)
    return data


def make_options(options: dict) -> str:
    """Format a dict as a LaTeX optional-argument string.

    Args:
        options: mapping of option name to value (None for flag-only).

    Returns:
        "[key=val,flag]" or "" when options is empty.
    """
    parts = []
    for k, v in sorted(options.items()):
        parts.append(k if v is None else f"{k}={v}")
    return f"[{','.join(parts)}]" if parts else ""


def _list_to_str(text) -> str:
    """Convert a string or list of tex objects to a single string.

    Args:
        text: a plain string or iterable of objects supporting __str__.

    Returns:
        Combined string.
    """
    if isinstance(text, str):
        return encode(text)
    return "".join(encode(i) if isinstance(i, str) else str(i) for i in text)


# ---------------------------------------------------------------------------
# Base classes
# ---------------------------------------------------------------------------

class TexFoundation:
    """Base for all LaTeX constructs."""

    def __init__(self, data):
        self._data = data

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    @property
    def tex_str(self) -> str:
        return _list_to_str(self.data)

    def replace_chars(self, data: str) -> str:
        return encode(data)

    def __add__(self, other):
        result = CompoundTexConstruct()
        result.append(self)
        result.append(other)
        return result

    def __str__(self) -> str:
        return self.tex_str


class SimpleTexConstruct(TexFoundation):
    """Construct rendered via a single ``%s`` pattern."""

    pattern = r"%s"

    def __str__(self) -> str:
        return self.pattern % self.replace_chars(self.tex_str)


class SimpleEnvironment(TexFoundation):
    r"""\\begin{tag} ... \\end{tag} wrapper."""

    tag = "NotImplemented"

    def __init__(self, data, **options):
        super().__init__(data)
        self.options = options

    def __str__(self) -> str:
        rv = f"\n\\begin{{{self.tag}}}{make_options(self.options)}\n"
        rv += self.tex_str + "\n"
        rv += f"\\end{{{self.tag}}}\n\n"
        return rv


class Container(SimpleEnvironment):
    """Mutable container of tex objects."""

    def __init__(self, **opts):
        super().__init__(None, **opts)
        self.content: list = []

    def append(self, data):
        self.content.append(data)

    def __iadd__(self, other):
        self.content.append(other)
        return self

    def __add__(self, other):
        self.content.append(other)
        return self

    def __iter__(self):
        yield from (str(i) for i in self.content)

    def __str__(self) -> str:
        return "".join(str(i) for i in self.content)


class CompoundEnvironment(Container):
    """Container that wraps its content in ``\\begin / \\end``."""

    def __str__(self) -> str:
        rv = f"\n\\begin{make_options(self.options)}{{{self.tag}}}\n"
        for item in self.content:
            rv += str(item) + "\n"
        rv += f"\\end{{{self.tag}}}\n"
        return rv


class CompoundTexConstruct:
    """Ordered collection of tex objects."""

    def __init__(self, data=None):
        self.content: list = [] if data is None else list(data)

    def append(self, data):
        self.content.append(data)

    def __iadd__(self, other):
        self.content.append(other)
        return self

    def __add__(self, other):
        self.content.append(other)
        return self

    def __iter__(self):
        yield from self.content

    def __str__(self) -> str:
        return "".join(str(o) for o in self.content)


class IntegerTexConstruct(SimpleTexConstruct):
    """Construct that validates its argument as an integer."""

    def __init__(self, data):
        try:
            data = int(data)
        except (ValueError, TypeError):
            raise TexError(TEX_MESSAGES["ERR_INT_REQUIRED"])
        super().__init__(data)


# ---------------------------------------------------------------------------
# Inline constructs
# ---------------------------------------------------------------------------

class Bold(SimpleTexConstruct):
    pattern = r"\textbf{%s}"


class Emph(SimpleTexConstruct):
    pattern = r"\emph{%s}"


class Inline(SimpleTexConstruct):
    pattern = r"\lstinline!%s!"


class Verb(SimpleTexConstruct):
    pattern = r"\verb!%s!"


class Url(SimpleTexConstruct):
    pattern = r"\url{%s}"


class Href(TexFoundation):
    r"""LaTeX \href{url}{description} hyperlink.

    Args:
        url: target URL.
        description: display text or list of tex objects.
    """

    def __init__(self, url: str, description):
        super().__init__(url)
        self.description = description

    def __str__(self) -> str:
        return r"\href{%s}{%s}" % (self.data, _list_to_str(self.description))


class Comment(SimpleTexConstruct):
    def __str__(self) -> str:
        return "%% %s" % self.tex_str


# ---------------------------------------------------------------------------
# Block constructs
# ---------------------------------------------------------------------------

class Paragraph(SimpleTexConstruct):
    def __str__(self) -> str:
        return f"{self.tex_str}\n"


class Chapter(SimpleTexConstruct):
    pattern = r"\chapter{%s}" + "\n"


class Section(SimpleTexConstruct):
    pattern = r"\section{%s}" + "\n"


class SubSection(SimpleTexConstruct):
    pattern = r"\subsection{%s}" + "\n"


class SubSubSection(SimpleTexConstruct):
    pattern = r"\subsubsection{%s}" + "\n"


class Title(SimpleTexConstruct):
    pattern = r"\title{%s}" + "\n"


class Latex(TexFoundation):
    """Raw LaTeX passthrough."""

    def __str__(self) -> str:
        return f"\n{self.tex_str}"


class Item(SimpleTexConstruct):
    pattern = "\n" + r"\item %s"


class Enumerate(CompoundEnvironment):
    """Ordered list environment."""

    tag = "enumerate"

    def __str__(self) -> str:
        rv = f"\n\\begin{make_options(self.options)}{{{self.tag}}}"
        for item in self.content:
            rv += str(item)
        rv += f"\n\\end{{{self.tag}}}\n\n"
        return rv


class Itemize(Enumerate):
    """Unordered list environment."""
    tag = "itemize"


class BlockQuote(SimpleEnvironment):
    """Block quotation (requires csquotes package)."""
    tag = "displayquote"


class LineBreak(IntegerTexConstruct):
    pattern = r"\linebreak[%s]" + "\n"

    def __init__(self, data=1):
        super().__init__(data)

    def __str__(self) -> str:
        return self.pattern % self.data


class NewPage(TexFoundation):
    def __init__(self):
        super().__init__("")

    def __str__(self) -> str:
        return r"\newpage" + "\n"


class Listing(SimpleEnvironment):
    """lstlisting code block.

    Args:
        text: source code content.
        language: programming language identifier.
    """
    tag = "lstlisting"

    def __init__(self, text: str, language: str, **options):
        super().__init__(text, **options)
        self.language = language

    def __str__(self) -> str:
        if self.language:
            self.options["language"] = self.language
        rv = f"\n\n\\begin{{{self.tag}}}{make_options(self.options)}\n"
        rv += str(self.data) + "\n"
        rv += f"\\end{{{self.tag}}}\n"
        return rv


class Verbatim(SimpleEnvironment):
    """Verbatim environment."""
    tag = "verbatim"

    def __str__(self) -> str:
        rv = f"\n\\begin{{{self.tag}}}{make_options(self.options)}\n"
        rv += str(self.data) + "\n"
        rv += f"\\end{{{self.tag}}}\n"
        return rv


class Image(TexFoundation):
    """LaTeX image with optional width, height, and alignment.

    Args:
        file_name: path to the image file (extension is stripped for pdflatex).
        text: unused caption placeholder.
        width: width in cm; -1 means 0.9 textwidth.
        height: height in cm.
        alignment: center, left, or right.
    """

    def __init__(
        self,
        file_name: str,
        text=None,
        width: float | None = None,
        height: float | None = None,
        alignment: str = "center",
    ):
        super().__init__(file_name)
        self.text = text
        self.width = width
        self.height = height
        alignment = alignment.lower()
        if alignment not in VALID_ALIGNMENTS:
            raise TexError(TEX_MESSAGES["ERR_INVALID_ALIGNMENT"])
        self.alignment = alignment
        self.unit = "cm"

    def _image_opts(self) -> str:
        parts = []
        if self.width == -1:
            parts.append("width=0.9\\textwidth")
        elif self.width is not None:
            parts.append(f"width={self.width}{self.unit}")
        if self.height is not None:
            parts.append(f"height={self.height}{self.unit}")
        return f"[{','.join(parts)}]" if parts else ""

    def _alignment_env(self) -> str:
        return {"center": "center", "right": "flushright", "left": "flushleft"}[
            self.alignment
        ]

    def __str__(self) -> str:
        opts = self._image_opts()
        env = self._alignment_env()
        # the full path, not just the stem: pdflatex compiles with its cwd
        # set to the output directory (see Builder._compile_latex), not
        # necessarily the directory the image path is relative to, so
        # dropping the directory here silently breaks any image that is not
        # sitting flat next to the .tex file
        path = str(self.data)
        return (
            f"\n\\begin{{{env}}}\n"
            f"\\includegraphics{opts}{{{path}}}\n"
            f"\\end{{{env}}}\n"
        )


class Heading(TexFoundation):
    """Dispatch to the appropriate sectioning command based on level.

    Args:
        data: heading text.
        level: sectioning depth; 1=chapter, 2=section, 3=subsection, 4=subsubsection.
    """

    _hierarchy = {
        1: Chapter,
        2: Section,
        3: SubSection,
        4: SubSubSection,
    }

    def __init__(self, data, level: int):
        if level not in self._hierarchy:
            raise TexError(TEX_MESSAGES["ERR_INVALID_LEVEL"])
        self.level = level
        super().__init__(data)

    def __str__(self) -> str:
        return str(self._hierarchy[self.level](self.data))


# ---------------------------------------------------------------------------
# Table constructs
# ---------------------------------------------------------------------------

class SimpleTable(TexFoundation):
    """Centred table rendered via the tabulate library.

    Args:
        data: list of rows (each row is a list of values).
        headings: optional column headings.
    """

    def __init__(self, data, headings=None):
        super().__init__(data)
        self.headings = headings

    def _tabulate(self, tablefmt="latex") -> str:
        if self.headings:
            return tabulate_mod.tabulate(
                self.data, headers=self.headings, tablefmt=tablefmt, numalign="right"
            )
        return tabulate_mod.tabulate(self.data, tablefmt=tablefmt, numalign="right")

    def __str__(self) -> str:
        return "\n\n\\begin{center}\n" + self._tabulate() + "\n\\end{center}"


class LongTable(TexFoundation):
    """Multi-page longtable with optional sparklines.

    Args:
        data: iterable of row dicts.
        field_spec: list of column spec dicts.
        align: table alignment; center, left, or right.
        font_size: optional LaTeX font-size environment name (e.g. small).
        unit: column width unit (default cm).
    """

    class Formatter(TexFoundation):
        def __init__(self, spec):
            self.spec = spec

    class FieldFormatter(Formatter):
        def __call__(self, row):
            data = row[self.spec["name"]]
            fmt = self.spec["format_"]
            return self.replace_chars(fmt.format(data))

    class SparkLineFormatter(Formatter):
        @property
        def _data(self):
            return self.spec["spark_data"]

        @property
        def width(self):
            return self.spec["width"]

        @property
        def height(self):
            return self.spec["height"]

        def child_data(self, row):
            master = self.spec["master"]
            child = self.spec["child"]
            value = self.spec["value"]
            d = [float(i[value]) for i in self._data if i[child] == row[master]]
            return [100.0 * i for i in scale(d)] if d else []

        def formatted_data(self, data) -> str:
            return "".join(f"({i},{v})" for i, v in enumerate(data))

        def tikz(self, row) -> str:
            raw = self.child_data(row)
            n = len(raw)
            if not n:
                return ""
            _max, _min = max(raw), min(raw)
            formatted = self.formatted_data(raw)
            xscale = self.width / n
            try:
                yscale = self.height / (_max - _min)
            except ZeroDivisionError:
                yscale = self.height
            return (
                f"\\begin{{tikzpicture}}[xscale={xscale}, yscale={yscale}]"
                f"\\draw[ultra thin] plot[] coordinates {{{formatted}}};"
                "\\end{tikzpicture}"
            )

        def __call__(self, row):
            return self.tikz(row)

    def __init__(self, data, field_spec: list, align: str = "center",
                 font_size: str | None = None, unit: str = "cm"):
        self.fields = field_spec
        self.formatters = [self._get_formatter(f) for f in field_spec]
        self.unit = unit
        self.align = align
        self.font_size = font_size
        super().__init__(data)

    def _get_formatter(self, field_spec):
        fmt_map = {"field": self.FieldFormatter, "sparkline": self.SparkLineFormatter}
        return fmt_map[field_spec["~>"]](field_spec)

    @property
    def _col_align(self) -> str:
        return {"right": "r", "left": "l"}.get(self.align, "c")

    @property
    def _env_spec(self) -> str:
        s = f"[{self._col_align}]{{"
        for f in self.fields:
            s += f["align"][0].upper() + "{" + str(f["width"]) + self.unit + "}"
        return s + "}"

    @property
    def _column_headings(self) -> str:
        s = r"\rowcolor{tableheader}"
        last = len(self.fields) - 1
        for i, f in enumerate(self.fields):
            ha = f["heading_align"][0].lower()
            title = encode(f["title"] if f["title"] is not None else f["name"])
            s += f"  \\multicolumn{{1}}{{{ha}}}{{\\textcolor{{tabletextcolor}}{{{title}}}}}"
            s += " &\n" if i < last else r" \\ \\[-1em]" + "\n"
        return s

    @property
    def _content(self) -> str:
        lines = []
        for row in self.data:
            lines.append("  " + " & ".join(f(row) for f in self.formatters) + r" \\")
        return "\n".join(lines) + "\n"

    def __str__(self) -> str:
        size_env = self.font_size or "normalsize"
        r = f"\n\\begin{{{size_env}}}\n"
        r += r"\setlength{\tabcolsep}{2pt}"
        r += f"\\begin{{longtable}}{self._env_spec}\n"
        r += self._column_headings
        r += self._content
        r += "\\end{longtable}\n"
        r += f"\\end{{{size_env}}}\n"
        return r


# ---------------------------------------------------------------------------
# Document
# ---------------------------------------------------------------------------

class Document(CompoundTexConstruct):
    """A complete LaTeX document.

    Assembles preamble and body; str(doc) returns the full .tex source.

    Args:
        doc_type: LaTeX document class (default article).
        font_size: base font size in pt; 10, 11, or 12 (default 11).
        paper_size: paper size identifier (default a4paper).
    """

    valid_doc_types = VALID_DOC_TYPES

    def __init__(
        self,
        doc_type: str = "article",
        font_size: int = 11,
        paper_size: str = "a4paper",
    ):
        super().__init__()
        self._doc_type = "article"
        self._font_size = 11
        self._paper_size = "a4paper"
        self.doc_type = doc_type
        self.font_size = font_size
        self.paper_size = paper_size
        # populated by LatexRenderer from the doc2 Document's title fields:
        # preamble_extra holds \title{}/\author{}/\date{}/\providecommand{...}
        # lines, title_command holds the \maketitle call itself, which has
        # to come after \begin{document}. Every class -- built in or a
        # project .cls -- is expected to give \maketitle its own meaning
        # rather than dkit needing to know a class-specific command name.
        self.preamble_extra: list[str] = []
        self.title_command: str | None = None
        self.packages: list[tuple] = [
            (None, "longtable"),
            (None, "graphicx"),
            (None, "listings"),
            (None, "csquotes"),
            (None, "hyperref"),
            ("table,xcdraw", "xcolor"),
            (None, "booktabs"),
            (None, "tikz"),   # sparkline columns in LongTable draw with it
            (None, "array"),  # \newcolumntype, for LongTable's L/C/R columns
        ]
        if self.doc_type in VALID_DOC_TYPES:
            # margin=2cm is explicit rather than geometry's own bare default
            # (also ~17cm on a4paper) so a table's column widths have a
            # documented text width to fit, e.g. BostonTables.columns().
            # Skipped for a custom class: it is expected to set its own page
            # geometry (ntt-article.cls does), and loading geometry a second
            # time with different options is a hard "option clash" error,
            # not a harmless override.
            self.packages.append(("margin=2cm", "geometry"))

    @property
    def doc_type(self) -> str:
        return self._doc_type

    @doc_type.setter
    def doc_type(self, value: str):
        # valid_doc_types is a list of known built-in classes, not an
        # allow-list: a project .cls file (assumed already installed and on
        # LaTeX's own search path) is exactly as valid a \documentclass as
        # "article", just not one this module can know the name of in
        # advance. Only reject something that could not be a class name at
        # all, since this string reaches \documentclass{...} unescaped.
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]*", value):
            raise TexError(TEX_MESSAGES["ERR_INVALID_DOCTYPE"])
        self._doc_type = value

    @property
    def font_size(self) -> int:
        return self._font_size

    @font_size.setter
    def font_size(self, value: int):
        if value not in VALID_FONT_SIZES:
            raise TexError(TEX_MESSAGES["ERR_INVALID_FONT_SIZE"])
        self._font_size = value

    @property
    def paper_size(self) -> str:
        return self._paper_size

    @paper_size.setter
    def paper_size(self, value: str):
        if value not in VALID_PAPER_SIZES:
            raise TexError(TEX_MESSAGES["ERR_INVALID_PAPER_SIZE"])
        self._paper_size = value

    def preamble(self) -> str:
        """Build and return the LaTeX preamble string.

        Returns:
            LaTeX preamble including documentclass and usepackage directives.
        """
        pkg_lines = "\n".join(
            f"\\usepackage[{opts}]{{{name}}}" if opts else f"\\usepackage{{{name}}}"
            for opts, name in self.packages
        )
        # LongTable's fixed-width columns (L{w}/C{w}/R{w}) are not anything
        # array defines on its own -- they need defining once, here, rather
        # than in every document that happens to use a LongTable. Needed
        # for a custom class too: nothing about a project .cls makes these
        # column types stop being LongTable's own notation.
        longtable_support = (
            r"\newcolumntype{L}[1]{>{\raggedright\arraybackslash}p{#1}}" "\n"
            r"\newcolumntype{C}[1]{>{\centering\arraybackslash}p{#1}}" "\n"
            r"\newcolumntype{R}[1]{>{\raggedleft\arraybackslash}p{#1}}" "\n"
        )
        if self.doc_type in VALID_DOC_TYPES:
            # LongTable's header row (\rowcolor{tableheader},
            # \textcolor{tabletextcolor}) needs these two colours from
            # somewhere. Skipped for a custom class: unlike the column
            # types above, a colour is exactly the kind of "look and feel"
            # choice a project .cls is expected to make for itself (as
            # ntt-article.cls does, in its own brand colours) --
            # \definecolor does not error on a second definition the way
            # \newcolumntype does, it silently overwrites, so redefining
            # these here would clobber the class's choice with no warning.
            longtable_support += (
                r"\definecolor{tableheader}{HTML}{D9D9D9}" "\n"
                r"\definecolor{tabletextcolor}{HTML}{000000}" "\n"
            )
        extra = "".join(line + "\n" for line in self.preamble_extra)
        return (
            f"\\documentclass[{self._font_size}pt,{self._paper_size}]"
            f"{{{self._doc_type}}}\n"
            f"{pkg_lines}\n"
            f"{longtable_support}"
            f"{extra}"
        )

    def __str__(self) -> str:
        body = "".join(str(o) for o in self.content)
        title = f"{self.title_command}\n" if self.title_command else ""
        return (
            self.preamble()
            + "\\begin{document}\n"
            + title
            + body
            + "\n\\end{document}\n"
        )
