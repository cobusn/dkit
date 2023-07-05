import sys
sys.path.insert(0, "..")  # noqa
from dkit.doc.reportlab_renderer import ReportLabRenderer
from dkit.doc import document
# from example_barplot import gg
from dkit.plot import ggrammar
from dkit.doc.document import Table
from dkit.data import fake_helper
from dkit.doc.lorem import Lorem

lorem = Lorem()


def build_doc():
    """create document"""
    d = document.Document("DataKit Reportlab Renderer", "Example Document", "Cobus Nel")
    d += document.Heading("Heading 1", 1)

    d += document.Heading("Heading 2", 2)
    d += lorem.paragraph()
    # d += lorem.unordered_list()
    d += lorem.paragraph()
    d += document.MD(
        """
        ##  heading 2
        This is **bold** and *italic* text.

        This is `inline` text.

        This is a [link](http://google.com)

        > This is an example of
        > a **block** quote

        And some source code:

        ```python
        import xml
        xml.parse()
        ```

        And verbatim text:

            Verbatim
            text

        """
    )
    d += document.LineBreak(0.5)
    d += document.Heading("Specific Features", 1)

    # URL
    # d += document.Heading("URL", 2)
    # d += document.Link("Google", "www.google.com", "title")

    # Plots
    d += document.Image("plots/plot.png", "Forecast", width=17, height=8)
    d += document.Image("plots/plotdata.pdf", "Forecast", width=17, height=8)
    d += document.Image("plots/plotdata.pdf", "Forecast")

    d += document.Paragraph(
        [
            document.Text("hahahah "),
            document.Link("Google", "www.google.com", "google")
        ]
    )
    # List
    l1 = document.List()
    l1.add_entry("one")
    l1.add_entry("two")
    l2 = document.List()
    l2.add_entry("next one")
    l2.add_entry("next two")
    l1.add_entry(l2)
    d += l1

    # Figure
    d += document.Heading("Figure", 1)

    data = [
        {"index": "jan", "sales": 15, "revenue": 20},
        {"index": "feb", "sales": 10, "revenue": 30},
        {"index": "mar", "sales": 13, "revenue": 25},
        {"index": "apr", "sales": 10, "revenue": 20},
        {"index": "may", "sales": 10, "revenue": 50},
        {"index": "jun", "sales": 10, "revenue": 20},
        {"index": "jul", "sales": 10, "revenue": 20},
    ]

    fig = document.Figure(data) \
        + ggrammar.GeomBar("Revenue", y_data="revenue", x_data="index",
                           color="#0000FF", alpha=0.8) \
        + ggrammar.Title("2018 Sales") \
        + ggrammar.YAxis("Rand", min=0, max=100, ticks=1) \
        + ggrammar.XAxis("Month") \
        + ggrammar.Aesthetic(stacked=True, height=5)

    d += fig

    # Table
    d += document.Heading("Tables", 1)
    d += Table(
        list(fake_helper.persons(n=20)),
        [
            Table.Field("first_name", "First name", width=3),
            Table.Field("last_name", "Last name", width=3),
            Table.Field("birthday", "DOB", align="right", width=3, format_="{:%Y-%m-%d}"),
            Table.Field("gender", "Gender", width=2),
        ]
    )

    return d


def render_doc(doc):
    b = ReportLabRenderer()  # local_style="stylesheet.yaml")
    b.run("reportlab_render.pdf", doc)


if __name__ == "__main__":
    render_doc(build_doc())
