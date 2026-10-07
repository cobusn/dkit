# Lib DKIT: A Data Processing Toolkit

This suite consists of two parts:

## Data Processing Library

Python data processing library that supports data processing (data engineering
and exploration).

The main principle is that data in formats are processed in a canonical format
(lists or iterators of dicts) and the library provides the facility to perform
translations between different data formats (e.g. csv, parquet, sql) and the
canonical format. It provides tools to support the process including:

- schema management
- reading and writing to data
- various supporting utilities:
  - logging
  - cli
  - programmable documents

## CLI Application for Data Processing

CLI application (`lib_dk/dk.py`) that has the following utility:

- convert from one format to another while optionally executing transforms
- database queries
- programmatic documents
- maintain data model schemas
- data manipulation including:
  - joins
  - pivot / melt
  - aggregation
- data exploration e.g:
  - summaries
  - histograms
  - plots
  - head / tail
  - counts
  - find duplicates
  - sample from database tables
  - explore data in a grid

The CLI application utilises the library.

## Version and Package

| Item            | Value                  |
|-----------------|------------------------|
| Current version | `26.4.2`               |
| Package name    | `libdkit`              |
| Entry point     | `dk` → `lib_dk/dk.py:main` |

Version follows CalVer (`YY.M.PATCH`). The source of truth is `dkit.__version__`.

## Package Structure

| Package               | Purpose                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| `dkit.algorithms`     | Trie, t-digest                                                          |
| `dkit.data`           | Aggregation, EDA, filtering, iteration, histogram, containers, diff, fake data, JSON/msgpack utilities, window functions, schema inference |
| `dkit.etl`            | Sources, sinks, transforms, ETL model, schema, verifier; extensions for Arrow, Avro, Parquet, SQLAlchemy, Pandas, Spark, Protobuf, XLS/XLSX, Athena |
| `dkit.doc2`           | Programmatic document builder — ReportLab, Docx, HTML, and Markdown renderers, Markdown-to-doc, project folder initialiser |
| `dkit.parsers`        | Parser helpers, URI parser, type parser                                 |
| `dkit.plot2`          | Declarative plotting on matplotlib: quick one-liners, geoms/scales/themes, canned analytical charts |
| `dkit.utilities`      | Benchmarking, cache, CLI helpers, concurrency, file helpers, security, SMTP (`SmtpMessage`, `DocumentMessage`), Jinja2, time, network, ZMQ, logging |
| `dkit.shell`          | Shell and config utilities                                              |
| `lib_dk`              | CLI modules: explore, transform, schema, aggregate, diff, build, run, queries, relations, connections, endpoints, store, admin, xml |

## Build and Install

```bash
pip install .          # install from repo root
pip install -e .       # editable install for development
```

- Build backend: `setuptools` via `pyproject.toml`.
- Non-Python resources (fonts, PDFs, CSVs, YAML, Markdown templates) are
  declared in `MANIFEST.in` and included via `include-package-data = true`.
- Resources are accessed at runtime via `importlib.resources`.

## Testing

Tests live in `test/`. Run from the repo root:

```bash
pytest                 # run all active tests
pytest --cov           # with coverage report
```

- Test files follow the `test_<module>.py` naming convention.
- Integration tests that require external services (HDFS, SFTP, ZMQ) are
  prefixed with `_test_` and excluded from the default test run.

## Document Rendering (`dkit.doc2`)

A `Document` object is the canonical intermediate format.  Content is added
via `add_element()` or `add_template()` (Markdown string) and then rendered
by passing the document to a renderer.

| Renderer          | Output              | Key method              |
|-------------------|---------------------|-------------------------|
| `HtmlRenderer`    | HTML                | `render_string()`, `render()`, `render_email_string()` |
| `MarkdownRenderer`| Markdown (GFM)      | `render_string()`, `render()` |
| `RlRenderer`      | PDF (ReportLab)     | `render()`              |
| `DocxRenderer`    | DOCX                | `render()`              |

### Email pipeline

`HtmlRenderer` accepts a `css` parameter (file path or raw string) and
produces CSS-class-based HTML.  `render_email_string()` inlines all CSS
rules via `premailer` so that email clients that strip `<style>` blocks
still render correctly.  Pass `inline_images=True` to embed local images
as base64 data URIs (suitable for browser preview).

`DocumentMessage` (in `dkit.utilities.smtp_helper`) is a `SmtpMessage`
subclass that accepts a `Document` and `css` path and handles all
rendering, plain-text fallback, and CID image embedding automatically:

```python
msg = DocumentMessage(
    subject="...", sender="...", recipients=[...],
    document=report, css="email.css",
)
```

### Outlook compatibility notes

- Use `<div class="header">` / `<div class="main">` rather than the
  HTML5 `<header>` / `<main>` elements.  Outlook's Word-based renderer
  ignores `background-color` on unknown semantic elements, causing the
  body background to bleed through.
- Embed images via CID MIME attachments (`src="cid:<token>"`), not data
  URIs.  Outlook does not render data URIs in email.  `DocumentMessage`
  handles this automatically.
- Avoid `height: auto` in CSS applied to `<img>` — Outlook interprets
  this as `height="auto"` on the element, rendering the image at zero
  height.

### Declarative document styles

`dkit.stylepack` provides renderer-independent style manifests with shared
colour, typography, page, and chart tokens. Native CSS, ReportLab layout,
DOCX, LaTeX, and matplotlib assets remain reviewable files in the style
distribution; style distributions do not provide configuration-driven Python
imports.

Style registrations are stored in the user's `~/.dk.ini` by the `dk styles`
commands. The distribution name, manifest path, and style-root fingerprint
are recorded so a later build can detect a missing or changed installation:

```console
dk styles register dkit-blue --distribution libdkit \
    --manifest dkit/stylepacks/dkit_blue/style.yaml
dk build doc --style dkit-blue --format reportlab \
    --title "Quarterly report" --output report.pdf report.md
```

Configured projects may select a style with `configuration.style` in
`report.yaml`; `[DOC] style` is the CLI configuration fallback. The selected
style is applied to ReportLab, HTML/email, DOCX, LaTeX, and scoped matplotlib
contexts. `dk styles preview NAME --output FOLDER` creates a cross-format
preview bundle. The built-in `styler` dotted path remains only as a deprecated
compatibility option.

## Conventions

### Config-file-driven instantiation

Classes that can be constructed from a YAML or dict config expose an inner
`Config(BaseModel)` Pydantic class. The standard instantiation pattern is:

```python
cfg = SomeClass.Config.model_validate(yaml_dict)
obj = SomeClass(**cfg.model_dump())
```
