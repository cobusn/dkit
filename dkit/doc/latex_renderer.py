from . import latex as tex
from .document import AbstractRenderer

from ..plot import matplotlib as mpl
from pathlib import Path


class LatexDocRenderer(AbstractRenderer):

    section_map = {
        0: tex.Chapter,
        1: tex.Section,
        2: tex.SubSection,
        3: tex.SubSubSection,
    }

    def __init__(self, data, style_sheet=None, plot_folder=None,
                 plot_backend=mpl.MPLBackend, root_container=None):
        if plot_folder is not None:
            self.plot_path = Path.cwd() / plot_folder
        else:
            self.plot_path = Path.cwd()
        super().__init__(data)
        self.stylesheet = style_sheet if style_sheet else {}
        self.plot_backend = plot_backend
        self.root = root_container if root_container else tex.Container()
        self.path = [self.root]

    def _append_node(self, node):
        """
        append node to node hierarchy
        """
        self.path[-1].append(node)
        return node

    def to_tex_list(self, elements):
        if isinstance(elements, str):
            return elements
        else:
            return [self.callbacks[e["~>"]](e) for e in elements]

    def encode_elements(self, elements):
        """iterate through top level elements, convert to tex and append to root"""
        for e in elements:
            _ = self.callbacks[e["~>"]](e)
            self._append_node(_)

    def make_bold(self, element):
        return tex.Bold(
            self.to_tex_list(element["data"])
        )

    def make_block_quote(self, element):
        return tex.BlockQuote(self.to_tex_list(element["data"]))

    def make_emphasis(self, element):
        return tex.Emph(self.to_tex_list(element["data"]))

    def make_figure(self, figure):
        filename = (self.plot_path / figure["filename"]).relative_to(Path().cwd())
        be = self.plot_backend(
            figure,
            terminal="pdf",
            style_sheet=self.stylesheet
        )
        be.render(filename)
        return tex.Image(filename)

    def make_heading(self, element):
        tex_elem = self.section_map[element["level"]]
        return tex_elem(self.to_tex_list(element["data"]))

    def make_inline(self, element):
        return tex.Inline(element["data"])

    def make_image(self, element):
        return tex.Image(
            element["data"],
            self.to_tex_list(element["data"])
        )

    def make_latex(self, element):
        return tex.Latex(element["data"])

    def make_line_break(self, element):
        return tex.LineBreak()

    def make_entry(self, element):
        return tex.Item(self.to_tex_list(element["data"]))

    def make_list(self, element):
        lst = tex.Enumerate() if element["ordered"] is True else tex.Itemize()
        for sub_element in element["data"]:
            lst.append(tex.Item(self.to_tex_list(sub_element["data"])))
        return lst

    def make_link(self, element):
        if element["data"] is not None:
            rv = tex.Href(element["url"], self.to_tex_list(element["data"]))
        else:
            rv = tex.Url(element["data"])
        return rv

    def make_listing(self, element):
        return tex.Listing(
            element["data"],
            element["language"]
        )

    def make_paragraph(self, element):
        return tex.Paragraph(self.to_tex_list(element["data"]))

    def make_table(self, data):
        if data["font_size"]:
            fsize = data["font_size"]
        elif "table" in self.stylesheet and "font_size" in self.stylesheet["table"]:
            fsize = self.stylesheet["table"]["font_size"]
        else:
            fsize = None

        return tex.LongTable(
            data["data"],
            data["fields"],
            align=data["align"],
            font_size=fsize
        )

    def make_text(self, element):
        return element["data"]

    def make_verbatim(self, element):
        return tex.Verbatim(element["data"].strip())

    def __iter__(self):
        self.encode_elements(self.data)
        yield from self.root

    def __str__(self):
        return "".join(self)


class LatexBeamerRenderer(LatexDocRenderer):

    section_map = {
        0: tex.Chapter,
        1: tex.FrameTitle,
        2: tex.FrameSubTitle,
    }

    def make_heading(self, element):
        level = element["level"]
        if level == 1:
            if len(self.path) > 1:
                # old block is completed and new one starts
                # yield the previous block content.
                self.path.pop()
            self.path.append(
                self._append_node(tex.Frame())
            )
            return tex.FrameTitle(
                self.to_tex_list(element["data"])
            )
        elif level == 2:
            return tex.FrameSubTitle(
                    self.to_tex_list(element["data"])
                )
        elif level == 3:
            pass
