****************
Getting Started
****************

This tutorial walks through the core dkit workflow end to end: install the
library, load a data file, infer a schema from it, convert it to another
format, and generate a short report from the result. It uses the sample
``titanic.csv`` file shipped in the project's ``examples/data`` directory,
but the same steps apply to any CSV, JSONL, or other supported source.

Install
=======

.. code-block:: bash

   pip install libdkit

dkit requires Python 3.11 or later.

Load a data file
=================

Every source in dkit yields an iterator of dictionaries — the canonical
format described in the :doc:`introduction`. Opening a source and looking
at the first few rows works the same way regardless of the underlying file
format:

.. code-block:: python

   from dkit.etl.model import ModelManager

   m = ModelManager.from_file(None)

   with m.source("examples/data/titanic.csv") as src:
       for row in list(src)[:3]:
           print(row)

``ModelManager.from_file(None)`` creates an empty, in-memory model — no
model YAML file is required to open a source directly.

Infer a schema
===============

Rather than writing a schema by hand, dkit can infer one from the data
itself. ``Entity.from_iterable`` samples the rows from a source and
returns an ``Entity`` describing each field's type:

.. code-block:: python

   from dkit.etl.model import Entity

   with m.source("examples/data/titanic.csv") as src:
       entity = Entity.from_iterable(src, infer_strings=True)

   print(entity)

``infer_strings=True`` tells the inference to recognise numeric values
that were stored as strings in the CSV (as CSV has no native type
information) — without it, every field is inferred as a string. The
result looks like:

.. code-block:: text

   PassengerId: Integer()
   Pclass: Integer()
   Name: String(str_len=59)
   Sex: String(str_len=6)
   Age: String(str_len=4)
   SibSp: Integer()
   Parch: Integer()
   Ticket: String(str_len=18)
   Fare: Float()
   Cabin: String(str_len=15)
   Embarked: String(str_len=1)

The inferred ``Entity`` can be stored on the model for reuse:

.. code-block:: python

   m.entities["passenger"] = entity

Run a conversion
==================

An ``Entity`` is callable: passing it an iterator of rows returns an
iterator of rows coerced to the entity's field types. Combined with a
source and a sink, this converts a file from one format to another while
applying the inferred schema — here, CSV to Parquet:

.. code-block:: python

   with m.source("examples/data/titanic.csv") as src:
       with m.sink("titanic.parquet") as snk:
           snk.process(entity(src))

The same pattern works for any source/sink pair dkit supports — swap the
file extensions to target JSONL, Avro, Excel, or a SQL table instead, with
no other changes to the code.

Generate a report
====================

``dkit.doc2`` builds a single canonical ``Document`` that can be rendered
to HTML, PDF, DOCX, or Markdown. A minimal report from a Markdown template
rendered to HTML looks like this:

.. code-block:: python

   from dkit.doc2.document import Document
   from dkit.doc2.html_renderer import HtmlRenderer

   doc = Document()
   doc.add_template("# Passenger data\n\nConverted **418** passenger records to Parquet.")

   renderer = HtmlRenderer(doc)
   renderer.render("report.html")

Swapping ``HtmlRenderer`` for ``RlRenderer`` or ``DocxRenderer`` renders
the same ``Document`` to PDF or DOCX instead — see
:doc:`howto_documents` for the full document-generation workflow,
including the HTML email pipeline.

Where to go next
===================

- :doc:`howto_etl_model` — connections, endpoints, and managing a full
  ETL project in a model file rather than a single script.
- :doc:`howto_documents` — the document renderers in more depth,
  including the email pipeline.
- :doc:`howto_cli_exploration` — doing the equivalent of this tutorial
  from the ``dk`` command line, without writing a script at all.
- The API reference pages, starting with :doc:`etl` and :doc:`data`.
