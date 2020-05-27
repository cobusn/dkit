#
# Example document with table and plot
#
import sys; sys.path.insert(0, "..") # noqa

from dkit.data import fake_helper
from dkit.doc import document
from dkit.doc.document import Table, Figure
from dkit.doc.latex_renderer import LatexDocRenderer
from sample_data import plot_data


d = document.Document()
d += document.Title("Document")
d += document.SubTitle("Sub title")
d += document.Author("Cobus Nel")
d += document.Heading("Heading 1", 1)
d += document.Heading("Heading 1.1", 2)
d += document.Heading("Heading 1.1.1", 3)
d += document.Paragraph("afda")
z = document.MD(
    """
    ## secttion 2.1
    This is a **markdown** document, with

    * multiple
    * options

    other `verbatim` components

    ```python
    verbatim
    ```
    code
    """
)

# d["clients"] = list(fake_helper.persons(n=20))
# d["plot"] = plot_data

d += document.Table(
    list(fake_helper.persons(n=20)),
    [
        Table.Field("first_name", "First name", width=5),
        Table.Field("last_name", "Last name"),
        Table.Field("birthday", "DOB", align="right", width=4, format_="%Y-%m-%d"),
        Table.Field("gender", "Gender"),
    ]
)

f = document.Figure(plot_data, "plotdata.pdf") \
    + Figure.Title("Sales per Month") \
    + Figure.GeomArea("Revenue", "index", "revenue", color="#0000FF", alpha=0.8)

d += f

# print(json.dumps(d.as_dict(), indent=4, default=str))

with open("content.tex", "w") as texfile:
    for st in LatexDocRenderer(d.as_dict()["elements"], plot_folder="plots"):
        texfile.write(st)
