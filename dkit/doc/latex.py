#
# Copyright (C) 2016  Cobus Nel
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
# FITNESS FOR A PARTICULAR PLRPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import os.path as path
import sys
import copy
import tabulate

"""
Latex helper classes
"""
# CONSTANTS
VALID_DOC_TYPES = ["article", "report", "book", "letter", "scrartcl"]
VALID_FONT_SIZES = [10, 11, 12]
VALID_PAPER_SIZES = ["a4paper", "letterpaper", "a5paper", "b5paper", "legalpaper", "executivepaper"]
VALID_ALIGNMENTS = ["center", "left", "right"]
TEX_MESSAGES = {
    "ERR_INVALID_DOCTYPE": "Invalid Document Type. Should be one of [%s]" % ", ".join(
        VALID_DOC_TYPES),
    "ERR_INVALID_FONT_SIZE": "Invalid Font Size. Should be one of [%s]" % ", ".join(
        [str(i) for i in VALID_FONT_SIZES]),
    "ERR_INVALID_PAPER_SIZE": "Invalid Paper Size. Should be one of [%s]" % ", ".join(
        VALID_PAPER_SIZES),
    "ERR_INVALID_ALIGNMENT": "Invalid Alignment specified. Should be one of [%s]" % ", ".join(
        VALID_ALIGNMENTS),
    "ERR_INT_REQUIRED": "Integer paramater required",
    "ERR_INVALID_LEVEL": "Invalid heading level specified"
    }

SPECIAL_CHARACTERS = {
    "#": r"\#",
    "&": r"\&",
    "%": r"\%",
    "£": r"\pounds",
    "€": r"\euro{}",
    "$": r"\$",
    "$": r"\$",
    "#": r"\#",
    "_": r"\_",
    "~": r"\textasciitilde",
    "^": r"\textasciicircum",
}


class TexError(Exception):
    pass


def list_2_str(text):
    """convert list of tex objects to string"""
    if isinstance(text, str):
        return encode(text)
    else:
        retval = ""
        for i in text:
            if isinstance(i, str):
                s = encode(i)
            else:
                s = str(i)
            retval += s
        return retval


def encode(data):
    """Replace special characters wiht latex equivalents"""
    for k, v in SPECIAL_CHARACTERS.items():
        if (k in data) and not (v in data):
            data = data.replace(k, v)
    return data


def make_options(options):
    """Make options
    Return opton in format "opt1=x,opt2=y"
    Return empty string if none
    """
    lst_options = []
    for k, v in sorted(options.items()):
        if v is None:
            lst_options.append(k)
        else:
            lst_options.append(f"{k}={v}")
    rv = ",".join(lst_options)
    if len(rv) > 0:
        return "[%s]" % rv
    else:
        return ""


def make_sweave_options(options):
    """Make options
    Return opton in format "opt1=x,opt2=y"
    Return empty string if none
    """
    rv = ",".join([k+"="+str(v) for k, v in options.items()])
    if len(rv) > 0:
        return "%s" % rv
    else:
        return ""


class TexFoundation(object):

    def __init__(self, data):
        self.__data = data

    def __get_data(self):
        return self.__data

    def __set_data(self, value):
        self.__data = value

    data = property(__get_data, __set_data, None, None)

    def __add__(self, other):
        retval = CompoundTexConstruct()
        retval.append(self)
        retval.append(other)
        return retval

    @property
    def tex_str(self):
        return list_2_str(self.data)

    def replace_chars(self, data):
        """Replace special characters wiht latex equivalents"""
        for k, v in SPECIAL_CHARACTERS.items():
            if (k in data) and not (v in data):
                data = data.replace(k, v)
        return data

    def __str__(self):
        return self.tex_str


class SimpleTexConstruct(TexFoundation):

    pattern = r"%s"

    def __str__(self):
        """section __str__"""
        return (self.pattern % self.replace_chars(self.tex_str))


class ComplexTexConstruct(object):

    def __str__(self):
        raise(NotImplementedError)


class SimpleEnvironment(TexFoundation):

    tag = "NotImplemented"

    def __init__(self, data, **options):
        self.data = data
        self.options = options

    def __str__(self):
        rv = "\n" + r"\begin{%s}%s" % (self.tag, make_options(self.options)) + "\n"
        rv += self.tex_str + "\n"
        rv += r"\end{%s}" % self.tag + "\n\n"
        return rv


class Container(SimpleEnvironment):

    def __init__(self, **opts):
        super().__init__(None, **opts)
        self.content = []

    def append(self, data):
        self.content.append(data)

    def __iadd__(self, other):
        self.content.append(other)
        return self

    def __add__(self, other):
        self.content.append(other)
        return self

    def __str__(self):
        return self.tex_str

    def __iter__(self):
        yield from (str(i) for i in self.content)


class CompoundEnvironment(Container):

    def __str__(self):
        rv = "\n" + r"\begin%s{%s}" % (make_options(self.options), self.tag) + "\n"
        for item in self.content:
            rv += str(item) + "\n"
        rv += r"\end{%s}" % self.tag + "\n"
        return rv


class Frame(CompoundEnvironment):
    tag = "frame"

    def __str__(self):
        rv = "\n" + r"\begin%s{%s}" % (make_options(self.options), self.tag) + "[fragile]\n"
        for item in self.content:
            rv += str(item) + "\n"
        rv += r"\end{%s}" % self.tag + "\n"
        return rv


class CustomEnvironment(SimpleEnvironment):
    """
    Take tag name from Class name
    """
    def __init__(self, data, **opts):
        super(CustomEnvironment, self).__init__(data, **opts)
        self.tag = self.__class__.__name__


class CompoundTexConstruct(ComplexTexConstruct):

    def __init__(self, data=None):

        # initialize content
        if data is None:
            self.content = []
        else:
            self.content = data

    def append(self, data):
        self.content.append(data)

    def __iadd__(self, other):
        self.content.append(other)
        return self

    def __iter__(self):
        yield from self.content

    def __add__(self, other):
        self.content.append(other)
        return self

    def __str__(self):
        retval = ""
        for o in self.content:
            retval += str(o)
        return retval


class IntegerTexConstruct(SimpleTexConstruct):
    """Latex construct with integer parameter"""

    def __init__(self, data):
        try:
            data = int(data)
        except ValueError:
            raise TexError(TEX_MESSAGES["ERR_INT_REQUIRED"])
        super(IntegerTexConstruct, self).__init__(data)


class Bold(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\textbf{%s}"


class BlockQuote(SimpleEnvironment):
    """lstlisting"""
    tag = "displayquote"


class Chapter(SimpleTexConstruct):
    """Latex chapter"""
    pattern = r"\chapter{%s}" + "\n"


class Comment(SimpleTexConstruct):
    """Latex Comment"""
    pattern = "%s"

    def __str__(self):
        return r"%%" + " %s" % self.tex_str


class FrameTitle(SimpleTexConstruct):
    """Beamer Frame Title"""
    pattern = r"\frametitle{%s}" + "\n"


class FrameSubTitle(SimpleTexConstruct):
    """Beamer Frame Title"""
    pattern = r"\framesubtitle{%s}" + "\n"


class UnDocument(CompoundTexConstruct):

    def __init__(self, file_obj=None, data=None):
        """
        Latex Document Class
        """
        super().__init__(data)
        if file_obj is None:
            self._file = sys.stdout
        else:
            self._file = file_obj

    def write(self):
        for o in self.content:
            self._file.write(str(o))

    def __str__(self):
        retval = ""
        for o in self.content:
            retval += str(o)
        return retval


class Document(UnDocument):

    valid_doc_types = VALID_DOC_TYPES

    def __init__(self, file_obj=None, data=None, doc_type="article", font_size=11,
                 paper_size="a4paper"):
        """
        Latex Document Class
        """
        super(Document, self).__init__(file_obj, data)
        if file_obj is None:
            self._file = sys.stdout
        else:
            self._file = file_obj
        self.__doc_type = "article"
        self.__font_size = 10
        self.__paper_size = "a4paper"
        self.doc_type = doc_type
        self.paper_size = paper_size
        self.font_size = font_size
        self.packages = [
            # First value is for options, should be string value
            (None, "longtable"),
            (None, "graphicx"),
            (None, "listings"),
            (None, "Sweave"),
            (None, "quotes"),        # for BlockQuotes
            (None, "hyperref")
        ]

    def get_doc_type(self):
        """
        Latex Document Type
        """
        return self.__doc_type

    def set_doc_type(self, value):
        if value not in self.valid_doc_types:
            raise TexError(TEX_MESSAGES["ERR_INVALID_DOCTYPE"])
        self.__doc_type = value

    doc_type = property(get_doc_type, set_doc_type, None, None)

    def get_font_size(self):
        """
        Documen font size
        """
        return self.__font_size

    def set_font_size(self, value):
        if value not in VALID_FONT_SIZES:
            raise TexError(TEX_MESSAGES["ERR_INVALID_FONT_SIZE"])
        self.__font_size = value

    font_size = property(get_font_size, set_font_size, None, None)

    # Paper Size
    def set_paper_size(self, value):
        """
        Document Paper Size
        """
        if value not in VALID_PAPER_SIZES:
            raise TexError(TEX_MESSAGES["ERR_INVALID_PAPER_SIZE"])
        self.__paper_size = value

    def get_paper_size(self):
        return self.__paper_size

    paper_size = property(get_paper_size, set_paper_size, None, None)

    def pre_amble(self):
        """Render Document command"""

        def build_packages(packages):
            retval = "\n"
            for package in packages:
                if package[0]:
                    p = "[%s]{%s}" % package
                else:
                    p = "{%s}" % package[1]
                retval = retval + r"\usepackage%s" % p + "\n"

            return retval

        options = "[%s,%s]" % (str(self.__font_size)+"pt", self.__paper_size)
        cmd = "{%s}" % self.__doc_type
        return r"\documentclass%s%s" % (options, cmd) + build_packages(self.packages)

    def write(self):
        self._file.write(self.pre_amble())
        self._file.write(r'\begin{document}' + '\n')
        super(Document, self).write()
        self._file.write('\n' + r'\end{document}')


class Emph(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\emph{%s}"


class Item(SimpleTexConstruct):
    """Latex enumeration item"""
    pattern = "\n" + r"\item %s"


class Latex(SimpleTexConstruct):
    """Latex section"""
    pattern = None

    def __str__(self):
        rv = "\n" + self.tex_str
        return rv


class Enumerate(CompoundEnvironment):

    tag = "enumerate"

    def __str__(self):
        rv = "\n" + r"\begin%s{%s}" % (make_options(self.options), self.tag)
        for item in self.content:
            rv += str(item)
        rv += "\n" + r"\end{%s}" % self.tag + "\n\n"
        return rv


class Href(SimpleTexConstruct):
    pattern = r"\href{%s}{%s}"

    def __init__(self, url, description):
        super(Href, self).__init__(url)
        self.description = description

    def __str__(self):
        return self.pattern % (
            self.data,
            list_2_str(self.description)
        )


class Image(TexFoundation):
    """Latex image"""

    def __init__(self, file_name, text=None,  width=None, height=None, alignment="center"):
        # Note self.text is reserved for future use
        super().__init__(file_name)
        self.text = text
        self.width = width
        self.height = height
        alignment = alignment.lower()
        if alignment not in VALID_ALIGNMENTS:
            raise TexError(TEX_MESSAGES["ERR_INVALID_ALIGNMENT"])
        self.alignment = alignment
        self.unit = "cm"

    def __get_image_opts(self):
        """Calculate image options"""
        options = ""
        if self.width is not None:
            options = options + "width=%s" % (str(self.width) + self.unit)
        if self.height is not None:
            if self.width is not None:
                options = options + ","
            options = options + "height=%s" % (str(self.height) + self.unit)
        if len(options) > 0:
            options = "[%s]" % options
        return options

    def __get_image_alignment(self):
        """Calculate latex alignment"""
        alignment = ""
        if self.alignment == "center":
            alignment = "center"
        elif self.alignment == "right":
            alignment = "flushright"
        elif self.alignment == "left":
            alignment = "flushleft"
        return alignment

    def __str__(self):
        """Format an image"""
        options = self.__get_image_opts()
        alignment = self.__get_image_alignment()
        base, ext = path.splitext(self.data)
        retval = ("\n" + r"\begin{%s}"+"\n") % alignment
        retval = retval + (r"\includegraphics%s{%s}"+"\n") % (options, base)
        retval = retval + (r"\end{%s}"+"\n") % alignment
        return retval


class Inline(SimpleTexConstruct):
    pattern = r"\lstinline!%s!"


class Itemize(Enumerate):
    tag = "itemize"


class Listing(SimpleEnvironment):
    """lstlisting"""
    tag = "lstlisting"

    def __init__(self, text, language, **options):
        super().__init__(text, **options)
        self.language = language

    def __str__(self):
        self.options.update({"language": self.language})
        rv = "\n\n" + r"\begin{%s}%s" % (self.tag, make_options(self.options)) + "\n"
        rv += str(self.data) + "\n"
        rv += r"\end{%s}" % self.tag + "\n"
        return rv


class LineBreak(IntegerTexConstruct):
    """Latex section"""
    pattern = r"\linebreak[%s]" + "\n"

    def __init__(self, data=1):
        super(LineBreak, self).__init__(data)

    def __str__(self):
        return self.pattern % self.data


class Literal(SimpleTexConstruct):
    """Do not modify"""
    pass


class Paragraph(SimpleTexConstruct):
    """paragraph"""
    def __str__(self):
        return f"{self.tex_str}\n"


class NewPage(SimpleTexConstruct):

    def __init__(self):
        super(NewPage, self).__init__("")
    pattern = r"\newpage" + "\n"

    def __str__(self):
        return self.pattern


class Section(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\section{%s}" + "\n"


class SubSection(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\subsection{%s}" + "\n"


class SubSubSection(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\subsubsection{%s}" + "\n"


class SubTitle(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\subtitle{%s}" + "\n"


class Sweave(SimpleTexConstruct):
    """Latex section"""

    def __init__(self, data, **opts):
        super(Sweave, self).__init__(data)
        self.options = opts

    def __str__(self):
        options = copy.copy(self.options)
        r = "\n\n<<%s>>==\n" % make_sweave_options(options)
        r += self.data
        r += "\n@\n"
        return r


class Title(SimpleTexConstruct):
    """Latex section"""
    pattern = r"\title{%s}" + "\n"


class LongTable(TexFoundation):

    class Formatter(TexFoundation):

        def __init__(self, spec):
            self.spec = spec

    class FieldFormatter(Formatter):

        def __call__(self, row):
            data = row[self.spec["name"]]
            fmt = self.spec["format_"]
            return self.replace_chars(fmt.format(data))

    class SparkBarFormatter(Formatter):

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
            return [float(i[value]) for i in self._data if i[child] == row[master]]

        def formatted_data(self, data):
            """return child data for row"""
            return "".join(["({},{})".format(i, v) for i, v in enumerate(data)])

        def tikz(self, row):
            raw = self.child_data(row)
            n = len(raw)
            _max = max(raw)
            _min = min(raw)
            formatted = self.formatted_data(raw)
            xscale = self.width / n
            yscale = self.height / (_max - _min)
            retval = (
                f"\\begin{{tikzpicture}}[xscale={xscale}, yscale={yscale}]"
                # f"\\draw[ultra thin, black!50] (-1,{_min})--({n},{_min});"
                # f"\\draw[ultra thin, black!50] (-1,{_max})--({n},{_max});"
                f"\\draw[ultra thin] plot[] coordinates {{{formatted}}};"
                "\\end{tikzpicture}"
            )
            return retval

        def __call__(self, row):
            return self.tikz(row)

    def __init__(self, data, field_spec, align="center", font_size=None,
                 unit="cm"):
        self.fields = field_spec
        self.formatters = [self.get_formatter(f) for f in field_spec]
        self.unit = unit
        self.align = align
        self.font_size = font_size
        super().__init__(data)

    def get_formatter(self, field_spec):
        fmt_map = {
            "field": self.FieldFormatter,
            "sparkline": self.SparkBarFormatter,
        }
        return fmt_map[field_spec["~>"]](field_spec)

    @property
    def alignment(self):
        if self.align == "right":
            return "r"
        elif self.align == "left":
            return "l"
        else:
            return "c"

    @property
    def environment_start(self):
        s = f"[{self.alignment}]"
        s += "{"
        for field in self.fields:
            s += field["align"][0].upper() + "{" + str(field["width"]) + self.unit + "}"
        s += "}"
        return s

    @property
    def column_headings(self):
        s = r"\rowcolor{tableheader}"
        length = len(self.fields) - 1
        for i, field in enumerate(self.fields):
            s += r"  \multicolumn{1}{" + field["heading_align"][0].lower() + "}{"
            if field["title"] is not None:
                s += self.replace_chars(field["title"])
            else:
                s += self.replace_chars(field["name"])
            if i < length:
                s += "} &\n"
            else:
                s += r"} \\ \\[-1em]" + "\n"
        return s

    @property
    def content(self):
        d = ""
        for i, row in enumerate(self.data):
            d += "  " + " & ".join([f(row) for f in self.formatters])
            d += r" \\" + "\n"
            # d += r" \\[2pt]" + "\n"
        return d

    def __str__(self):
        r = "\n"
        if self.font_size is not None:
            r += f"\\begin{{{self.font_size}}}\n"
        else:
            r += r"\begin{normalsize}" + "\n"
        r += r"\setlength{\tabcolsep}{2pt}"
        r += r"\begin{longtable}" + self.environment_start + "\n"
        r += self.column_headings
        r += self.content
        r += r"\end{longtable}" + "\n"
        if self.font_size is not None:
            r += f"\\end{{{self.font_size}}}\n"
        else:
            r += r"\end{normalsize}" + "\n"
        return r


class SimpleTable(TexFoundation):

    def __init__(self, data, headings=None):
        super().__init__(data)
        self.headings = headings

    def _tabulate(self,  tablefmt="latex"):
        """
        Return tabular string.

        refer to tabular documentation for value of tablefmt::

            https://pypi.python.org/pypi/tabulate

        """
        if self.headings:
            return tabulate.tabulate(self.data, headers=self.headings,
                                     tablefmt=tablefmt, numalign="right")
        else:
            return tabulate.tabulate(self.data, tablefmt=tablefmt, numalign="right")

    def __str__(self):
        return "\n\n" + r"\begin{center}" + "\n" + self._tabulate() + "\n" + r"\end{center}"


class Url(SimpleTexConstruct):
    pattern = r"\url{%s}"


class Verb(SimpleTexConstruct):
    pattern = r"\verb!%s!"


class Verbatim(SimpleEnvironment):
    """verbatim environment"""
    tag = "verbatim"

    def __str__(self):
        rv = "\n" + r"\begin{%s}%s" % (self.tag, make_options(self.options)) + "\n"
        rv += str(self.data) + "\n"
        rv += r"\end{%s}" % self.tag + "\n"
        return rv


class Heading(SimpleTexConstruct):

    hierarchy = {
        0: None,    # reserved for part
        1: Chapter,
        2: Section,
        3: SubSection,
        4: SubSubSection,
    }

    def __init__(self, data, level):
        if level not in self.hierarchy.keys():
            raise TexError(TEX_MESSAGES["ERR_INVALID_LEVEL"])
        self.level = level
        super(Heading, self).__init__(data)

    def __str__(self):
        return self.hierarchy[self.level](self.data).__str__()
