"""
example_html_email_content.py
==============================
Demonstrates how to generate email-ready HTML using HtmlRenderer.

The example covers:
  - Building a Document with headings, paragraphs, a table, and an image
  - Supplying a CSS stylesheet (inline string) for styling
  - Embedding a local image as a base64 data URI (inline_images=True)
  - Calling render_email_string() to produce CSS-inlined HTML safe for
    all email clients (Gmail, Outlook, Apple Mail, etc.)
  - Wiring the result into SmtpMessage

To send the email, replace the SmtpClient.Config values with real server
details and call client.send(msg).
"""
import sys; sys.path.insert(0, "..")  # noqa

from dkit.doc2 import document as doc
from dkit.doc2.html_renderer import HtmlRenderer
from dkit.etl import source


# ------------------------------------------------------------------
# Email stylesheet
# A plain CSS string.  Pass a file path ending in .css instead if
# you prefer to keep styles in a separate file.
# ------------------------------------------------------------------
EMAIL_CSS = """
body {
    font-family: Arial, Helvetica, sans-serif;
    font-size: 14px;
    color: #222222;
    background-color: #f4f4f4;
    margin: 0;
    padding: 0;
}
main {
    max-width: 640px;
    margin: 24px auto;
    background: #ffffff;
    padding: 32px;
    border-radius: 4px;
}
header {
    background-color: #003366;
    color: #ffffff;
    padding: 24px 32px;
    max-width: 640px;
    margin: 0 auto;
    border-radius: 4px 4px 0 0;
}
.doc-title {
    margin: 0;
    font-size: 22px;
    color: #ffffff;
}
.doc-subtitle {
    margin: 4px 0 0;
    font-size: 14px;
    color: #cce0ff;
}
.doc-author {
    margin: 2px 0 0;
    font-size: 12px;
    color: #99bbdd;
}
.heading-2 {
    color: #003366;
    border-bottom: 1px solid #dddddd;
    padding-bottom: 4px;
}
.heading-3 {
    color: #336699;
}
p {
    line-height: 1.6;
    margin: 0 0 12px;
}
blockquote {
    border-left: 4px solid #003366;
    margin: 0 0 12px 0;
    padding: 8px 16px;
    color: #555555;
    background: #f0f4fa;
}
table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 16px;
    font-size: 13px;
}
th {
    background-color: #003366;
    color: #ffffff;
    padding: 8px;
    text-align: center;
}
td {
    padding: 6px 8px;
    border-bottom: 1px solid #eeeeee;
}
tr:nth-child(even) td {
    background-color: #f9f9f9;
}
.image-center {
    text-align: center;
    margin: 16px 0;
}
figcaption {
    font-size: 11px;
    color: #888888;
    margin-top: 4px;
}
.page-break {
    border-top: 1px dashed #cccccc;
    margin: 24px 0;
}
"""

# ------------------------------------------------------------------
# Document content
# ------------------------------------------------------------------
report = doc.Document(
    title="Monthly Temperature Report",
    sub_title="Nottem Dataset Summary",
    author="Data Team",
)

report.add_template("""
## Overview

This report summarises the **Nottem** average air temperatures recorded at
Nottingham Castle.  The data covers monthly observations and is provided
here as a representative example of email-ready HTML output from dkit.

> All temperatures are in degrees Fahrenheit.

## Data Table

The table below shows the first twelve monthly records.
""")

# Table from the nottem dataset
with source.load("examples/data/nottem_temp.jsonl") as infile:
    temperature_data = list(infile)[:12]

report.add_element(
    doc.Table(
        data=temperature_data,
        columns=[
            doc.Column("Year",  "Year",        align="center"),
            doc.Column("Month", "Month",       align="center"),
            doc.Column("Temp",  "Temperature", align="right"),
        ],
        align="center",
    )
)

report.add_template("""
## Notes

- Data source: Nottem dataset (built-in R dataset).
- Values represent monthly mean air temperatures.
- Report generated automatically by **dkit HtmlRenderer**.
""")

# ------------------------------------------------------------------
# Render to HTML
# ------------------------------------------------------------------
renderer = HtmlRenderer(
    report,
    css=EMAIL_CSS,
    inline_images=True,   # embed any local images as data URIs
)

renderer.render("examples/example_render.html")
