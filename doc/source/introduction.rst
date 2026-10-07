************
Introduction
************

What is dkit
============

dkit is a Python library for data engineering and data exploration. It is
built around a single idea: data is handled in a **canonical format** —
lists or iterators of dictionaries — and the library provides the
translations between that canonical format and a wide range of storage
formats (CSV, JSON/JSONL, Parquet, Avro, Excel, SQL, HDF5, XML, Protocol
Buffers and more). Because every source and sink speaks the same
canonical format, converting between any two supported formats does not
require a dedicated converter for every pair — you read into the
canonical format once, and write out of it once.

On top of that core, dkit adds the tooling that a real data-processing
job usually needs alongside the conversion itself: schema management,
data manipulation (joins, pivots, aggregation), data exploration
(summaries, histograms, plots), and programmatic report generation. A
command-line tool, ``dk``, exposes most of this functionality directly
from the shell for interactive and scripted use.

Core ideas
==========

Canonical format
-----------------

Sources read data into an iterator of dictionaries; sinks write an
iterator of dictionaries out to a target format. Because both ends of
every pipeline agree on this shape, sources and sinks can be freely mixed
— a CSV source can feed a Parquet sink, a SQL source can feed a JSONL
sink — without any format-pair-specific glue code. Working with
iterators throughout also means dkit can process data larger than
memory: rows flow through a pipeline one at a time rather than being
loaded wholesale.

Schema management
------------------

An ``Entity`` describes the fields of a dataset — their names, types,
and constraints — using a compact shorthand encoding (for example
``String(str_len=20)``) and is validated using Pydantic. Schemas can be
inferred automatically from existing data, or written by hand, and once
defined can be exported to the type systems of other tools: Apache
Arrow, Apache Avro, SQL DDL (via SQLAlchemy, for any supported dialect),
Apache Spark, Pandas DataFrame dtypes, Python dataclasses, and GraphViz
entity-relation diagrams.

ETL model
---------

For anything beyond a one-off conversion, a ``ModelManager`` collects
the moving parts of a data project — connections, endpoints, entities,
queries, transforms, relations, and encrypted secrets — into a single
YAML model file. This gives a project a single place to look for "what
data sources exist and how are they related", rather than scattering
connection strings and schema definitions across scripts.

What's in the box
==================

``dkit.etl``
    Sources, sinks, transforms, the ETL model, and schema validation —
    the core data-movement layer described above.

``dkit.etl.extensions``
    Format-specific implementations that plug into ``dkit.etl``: Arrow,
    Avro, Parquet, SQLAlchemy, Pandas, Spark, Protocol Buffers,
    Excel (XLS/XLSX), Athena, and HDF5.

``dkit.data``
    Data-manipulation and exploration utilities that operate on the
    canonical format directly: aggregation, exploratory data analysis,
    filtering, histograms, window functions, schema inference, and fake
    data generation for testing.

``dkit.doc2``
    A programmatic document builder. A single canonical ``Document`` can
    be rendered to PDF (via ReportLab), DOCX, HTML, or Markdown, which
    makes it useful for generating reports and HTML email from the same
    source content.

``dkit.algorithms``
    General-purpose data structures used elsewhere in the library — a
    trie and a t-digest implementation for streaming quantile
    estimation.

``dkit.parsers``
    Parsing helpers: a URI parser, a type parser, and supporting
    utilities used throughout the ETL and schema layers.

``dkit.plot2``
    Declarative plotting on matplotlib: quick one-liners for common
    charts, a layered geom/scale/theme grammar for anything more bespoke,
    and canned analytical charts (control charts, pareto, histograms,
    quadrant/growth-share).

``dkit.utilities``
    Supporting infrastructure: logging, CLI helpers, SMTP/email
    (including the ``doc2`` email pipeline), Jinja2 templating,
    security/encryption, ZMQ messaging, concurrency, and benchmarking.

``dkit.shell``
    Shell and configuration utilities used by the ``dk`` CLI and by
    interactive tooling.

``dk`` (the ``lib_dk`` package)
    The command-line front end for the library. It exposes schema
    management, ETL pipelines, data exploration, diffing, and document
    building as subcommands, so most of the library's functionality is
    reachable without writing a script.

Who it's for
=============

dkit is aimed at data engineers who write format-conversion and ETL
scripts and want a consistent, format-agnostic way to do it, as well as
at anyone who wants a lightweight way to generate reports or HTML email
from Python without pulling in a full BI or templating stack.

Where to go next
=================

If you are new to dkit, the :doc:`tutorial` walks through installing the
library, loading data, inferring a schema, running a conversion, and
generating a report. For specific tasks, see the how-to guides:
:doc:`howto_etl_model`, :doc:`howto_documents`, and
:doc:`howto_cli_exploration`. The remaining pages document the library's
API in full.
