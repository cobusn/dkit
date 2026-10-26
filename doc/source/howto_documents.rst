*******************************************
How to: document generation & email reports
*******************************************

The :doc:`tutorial` rendered a single Markdown template to HTML. This
guide covers the rest of ``dkit.doc2``: building a document from mixed
content (Markdown text, tables, images), rendering the same document to
PDF, DOCX, and Markdown as well as HTML, and sending it as an HTML email
with embedded images via the ``dkit.utilities.smtp_helper`` pipeline.

Everything in ``doc2`` is built around one idea: a :class:`Document
<dkit.doc2.document.Document>` is a renderer-agnostic tree of content
elements. You build the document once, then pick a renderer for the
output format you need — nothing about the document itself changes
between HTML, PDF, DOCX, or Markdown output.

Building a document
======================

A ``Document`` is created with optional title-page metadata, and content
is added either as a Markdown string or as individual elements:

.. code-block:: python

   from dkit.doc2.document import Document

   doc = Document(
       title="Passenger Report",
       sub_title="Titanic dataset",
       author="dkit",
       contact="reports@example.com",
   )

   doc.add_template("""
   # Passenger data

   Summary of the titanic dataset.

   - 418 rows
   - 11 fields
   """)

``add_template`` parses a Markdown string (via ``mistune``) into
``doc2``'s internal element tree and appends the result to the
document. ``add_template_files`` does the same for a list of files, if
your report content is easier to maintain as separate ``.md`` files.

Adding a table
================

Markdown text covers headings, paragraphs, lists, and inline formatting,
but a data table is added directly as a :class:`Table
<dkit.doc2.document.Table>` element rather than through Markdown table
syntax:

.. code-block:: python

   from dkit.doc2.document import Table, Column

   table = Table(
       data=[
           {"name": "Age", "type": "float"},
           {"name": "Fare", "type": "float"},
       ],
       columns=[
           Column(name="name", title="Field"),
           Column(name="type", title="Type"),
       ],
   )
   doc.add_element(table)

Each :class:`Column <dkit.doc2.document.Column>` maps one key in the row
dictionaries to a display column, with its own title, width, alignment,
and format string — see the ``Column`` and ``Table`` API reference for
the full set of options.

Adding an image
==================

An :class:`Image <dkit.doc2.document.Image>` element references a local
file path or a remote URL:

.. code-block:: python

   from dkit.doc2.document import Image

   doc.add_element(Image(source="examples/python-logo.png", title="logo"))

Images can also be embedded directly from a Markdown template using the
``image`` Jinja helper that every ``Document`` exposes to
``add_template``: ``{{ image("path/to/file.png", title="...") }}``. The
helper accepts the same options as the ``Image`` element itself —
``title``, ``width``, ``height`` (both in cm), and ``align`` (``"left"``,
``"center"``, or ``"right"``, defaulting to ``"center"``):

.. code-block:: text

   {{ image("examples/python-logo.png", title="logo", width=4, align="left") }}

Forcing a page break
=======================

The ``page_break`` Jinja helper inserts a :class:`PageBreak
<dkit.doc2.document.PageBreak>` element directly from a Markdown
template:

.. code-block:: text

   First page content.

   {{ page_break() }}

   Second page content.

.. note::
   ``threshold`` (in cm) makes the break conditional: ``{{
   page_break(threshold=2.5) }}`` only starts a new page if less than
   ``2.5`` cm of vertical space remains on the current page, instead of
   always breaking. This is only honoured by ``RLRenderer`` (PDF
   output) — the other three renderers ignore ``threshold`` and always
   break.

Including file contents as a code block
===========================================

The ``include`` Jinja helper reads a file's contents and inserts them
as a :class:`CodeBlock <dkit.doc2.document.CodeBlock>` — useful for
embedding a source listing without pasting it into the Markdown
template by hand:

.. code-block:: text

   {{ include("examples/report.py", language="python") }}

``language`` is optional and is used by renderers that support syntax
highlighting for the code block.

Adding math notation
======================

LaTeX-style math is supported directly in Markdown templates added via
``add_template``: inline math with ``$...$`` and display (block) math
with ``$$...$$``:

.. code-block:: text

   Energy and mass are related by $E=mc^2$.

   $$
   I = \int \rho R^{2} dV
   $$

A :class:`Math <dkit.doc2.document.Math>` element can also be added
directly, the same way as any other element:

.. code-block:: python

   from dkit.doc2.document import Math

   doc.add_element(Math("E=mc^2", display=False))

Display math is centered by default; pass ``align="left"`` or
``align="right"`` to override (inline math always stays inline and
ignores ``align``):

.. code-block:: python

   doc.add_element(Math("I = \\int \\rho R^{2} dV", display=True, align="left"))

Math renders natively (real TeX) only in ``LatexRenderer`` output. The
other three renderers (``HtmlRenderer``, ``RLRenderer``, ``DocxRenderer``)
rasterise each math span with matplotlib's ``mathtext`` — as SVG, PDF,
and PNG respectively — which supports a useful subset of LaTeX math, not
the full TeX math engine (for example, no ``\begin{align}`` and limited
macro support).

Rendering
===========

Each output format has its own renderer, and every renderer takes the
same ``Document`` as its only required argument:

.. code-block:: python

   from dkit.doc2.html_renderer import HtmlRenderer
   from dkit.doc2.rl_renderer import RLRenderer
   from dkit.doc2.docx_renderer import DocxRenderer
   from dkit.doc2.md_renderer import MarkdownRenderer

   HtmlRenderer(doc).render("report.html")
   RLRenderer(doc).render("report.pdf")
   DocxRenderer(doc).render("report.docx")
   MarkdownRenderer(doc).render("report.md")

``render(file_name)`` writes to a file; ``render_string()`` (available
on every renderer) returns the rendered output as a string instead,
which is how the email pipeline below builds its HTML body without
writing a temporary file.

.. warning::
   ``RLRenderer`` (PDF output) draws ``title``, ``sub_title``, and
   ``author`` directly onto the title page and does not currently
   guard against any of them being ``None``. Supply all three when
   using ``RLRenderer`` — as in the example above — or the render call
   raises an ``AttributeError``. This does not affect the other three
   renderers, which handle missing title fields correctly.

``HtmlRenderer`` additionally accepts a ``css`` argument (a raw CSS
string, or a path to a ``.css`` file) to style the HTML output, and a
``fragment=True`` flag to emit only the body content without the
``<!DOCTYPE html>`` wrapper, for embedding in a larger page.

Sending an HTML email
========================

``dkit.utilities.smtp_helper.DocumentMessage`` builds a complete email
from a ``Document`` in one step: a plain-text fallback body (via
``MarkdownRenderer``), an HTML body with CSS inlined for email-client
compatibility (via ``HtmlRenderer.render_email_string()``), and any
local images referenced by ``Image`` elements attached as CID MIME
parts so they display in Outlook without remote-image warnings:

.. code-block:: python

   from dkit.utilities.smtp_helper import DocumentMessage, SmtpClient

   message = DocumentMessage(
       subject="Passenger report",
       sender="reports@example.com",
       recipients=["team@example.com"],
       document=doc,
       css="email.css",
   )

   client = SmtpClient(server="smtp.example.com", port=587, use_tls=True,
                        authenticate=True, username="reports", password="...")
   client.send(message)

Local images are rewritten to ``cid:`` references automatically —
remote ``http://``/``https://`` image sources are left untouched, since
they load directly in the recipient's mail client.

Where to go next
===================

- :doc:`howto_etl_model` — generate the data a report summarises from
  an ETL model rather than a static list.
- :doc:`etl` and the ``dkit.doc2`` API reference for the full set of
  content elements and renderer options.
