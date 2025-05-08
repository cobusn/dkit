# Copyright (c) 2025 Cobus Nel
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
Utilities for workinw with docx and Word Documents
"""
import docx
from docx.shared import Pt
from docx.enum.style import WD_STYLE_TYPE
from docx.shared import Cm, Inches

from pydantic import BaseModel


class DocxConfig(BaseModel):
    """For configuration of DocxRenderer"""
    sty_normal: str = "Normal"
    sty_code_block: str = "CodeBlock"
    sty_bullet: str = "Bullet"
    sty_number_list: str = "Number"
    sty_quote: str = "Quote"
    sty_table: str = "Table Grid"


def create_codeblock_style(doc: docx.Document):
    """return a CodeBlock style. Create it if needed"""
    styles = doc.styles  # Access the document's styles object
    if "CodeBlock" not in styles:
        custom_body_style = styles.add_style('CodeBlock', WD_STYLE_TYPE.PARAGRAPH)
        body_font = custom_body_style.font
        body_font.name = 'Courier New'
        body_font.size = Pt(12)
        body_font.bold = False
        body_font.italic = False
        body_font.underline = False
        body_paragraph_format = custom_body_style.paragraph_format
        body_paragraph_format.space_before = Inches(0)
        body_paragraph_format.space_after = Inches(0.08)  # After paragraph spacing


def get_or_create_codeblock_style(doc: docx.Document):
    """return a CodeBlock style. Create it if needed"""
    breakpoint()
    if "BlockCode" not in doc.styles:
        styles = document.styles
        style = styles.add_style('BlockCode', WD_STYLE_TYPE.PARAGRAPH)
        # style.unhide_when_used = True
        font = style.font
        font.name = 'Courier New'  # Monospace
        font.size = Pt(10)
        font.italic = True

        # Add a background color (using shading)
        paragraph_format = style.paragraph_format
        paragraph_format.first_line_indent = Cm(1)
        paragraph_format.left_indent = Cm(1)
        paragraph_format.right_indent = Cm(1)

    return "BlockCode"


def get_or_create_hyperlink_style(d):
    """If this document had no hyperlinks so far, the builtin
       Hyperlink style will likely be missing and we need to add it.
       There's no predefined value, different Word versions
       define it differently.
       This version is how Word 2019 defines it in the
       default theme, excluding a theme reference.
    """
    if "Hyperlink" not in d.styles:
        if "Default Character Font" not in d.styles:
            ds = d.styles.add_style("Default Character Font",
                                    docx.enum.style.WD_STYLE_TYPE.CHARACTER,
                                    True)
            ds.element.set(docx.oxml.shared.qn('w:default'), "1")
            ds.priority = 1
            ds.hidden = True
            ds.unhide_when_used = True
            del ds
        hs = d.styles.add_style("Hyperlink",
                                docx.enum.style.WD_STYLE_TYPE.CHARACTER,
                                True)
        hs.base_style = d.styles["Default Character Font"]
        hs.unhide_when_used = True
        hs.font.color.rgb = docx.shared.RGBColor(0x05, 0x63, 0xC1)
        hs.font.underline = True
        del hs

    return "Hyperlink"


def add_hyperlink(paragraph, text, url):
    """add hyperlink to a paragraph

    source: https://stackoverflow.com/questions/47666642/adding-an-hyperlink-in-msword-by-using-python-docx"  # noqa
    """
    # This gets access to the document.xml.rels file and gets a new relation id value
    part = paragraph.part
    r_id = part.relate_to(url, docx.opc.constants.RELATIONSHIP_TYPE.HYPERLINK, is_external=True)

    # Create the w:hyperlink tag and add needed values
    hyperlink = docx.oxml.shared.OxmlElement('w:hyperlink')
    hyperlink.set(docx.oxml.shared.qn('r:id'), r_id, )

    # Create a new run object (a wrapper over a 'w:r' element)
    new_run = docx.text.run.Run(
        docx.oxml.shared.OxmlElement('w:r'), paragraph)
    new_run.text = text
    new_run.style = get_or_create_hyperlink_style(part.document)
    # Alternatively, set the run's formatting explicitly
    # new_run.font.color.rgb = docx.shared.RGBColor(0, 0, 255)
    # new_run.font.underline = True

    # Join all the xml elements together
    hyperlink.append(new_run._element)
    paragraph._p.append(hyperlink)
    return hyperlink


document = docx.Document()
p = document.add_paragraph('A plain paragraph having some ')
add_hyperlink(p, 'Link to my site', "http://supersitedelamortquitue.fr")
document.save('demo_hyperlink.docx')
