# DKit — Data Toolkit

A Python data-processing library and CLI for data engineering and exploration.
Data is handled in a canonical format (iterators or lists of dicts) and the
library provides facilities to translate between that format and a wide variety
of storage formats, manage schemas, manipulate data, and generate reports.

**Version:** 26.7.1 | **Requires:** Python ≥ 3.11 | **License:** MIT

---

## Bundled fonts

This project bundles Source Sans Pro fonts by Adobe, licensed under the
[SIL Open Font License 1.1](dkit/resources/OFL-1.1.txt).

---

## Installation

```bash
pip install libdkit          # from PyPI
pip install .                # from source
pip install -e .             # editable install for development
```

---

## Library overview

| Package | Purpose |
|---|---|
| `dkit.etl` | Sources, sinks, transforms, schema management, ETL model |
| `dkit.etl.extensions` | Format extensions: Arrow, Avro, Parquet, SQLAlchemy, Pandas, Spark, REST, Protobuf, XLS/XLSX, Athena, HDF5 |
| `dkit.data` | Aggregation, EDA, filtering, histograms, window functions, schema inference, fake data |
| `dkit.doc2` | Programmatic document builder — PDF (ReportLab), DOCX, HTML, Markdown renderers; email pipeline |
| `dkit.algorithms` | Trie, t-digest |
| `dkit.parsers` | URI parser, type parser, parser helpers |
| `dkit.plot2` | Declarative plotting on matplotlib: quick one-liners, geoms/scales/themes, canned analytical charts |
| `dkit.utilities` | Logging, CLI helpers, SMTP/email, Jinja2, security, ZMQ, concurrency, benchmarking |
| `dkit.shell` | Shell and config utilities |

### Supported data formats

| Format | Read | Write |
|---|---|---|
| JSON / JSONL | yes | yes |
| CSV | yes | yes |
| Apache Parquet | yes | yes |
| Apache Avro | yes | yes |
| MessagePack | yes | yes |
| Excel (XLS/XLSX) | yes | yes |
| SQL (via SQLAlchemy) | yes | yes |
| HDF5 | yes | yes |
| XML | yes | — |
| Protocol Buffers | yes | yes |

### Schema management

Schemas are stored in a compact shorthand encoding (e.g. `String(str_len=20)`)
and validated using Pydantic. Export is supported for:

- Apache Arrow
- Apache Avro
- SQL DDL (via SQLAlchemy, any supported dialect)
- Apache Spark
- Pandas DataFrame dtypes
- Python dataclasses
- GraphViz entity-relation diagrams

### ETL model

The `ModelManager` maintains connections, endpoints, entities, queries,
transforms, relations, and encrypted secrets in a YAML model file.

```python
from dkit.etl.model import ModelManager, Entity

m = ModelManager.from_file("model.yml")

# infer a schema from data
with m.source("data/input.jsonl") as src:
    entity = Entity.from_iterable(src)
m.entities["person"] = entity

# coerce CSV data to schema and write to parquet
with m.source("data/input.csv") as src:
    with m.sink("output/result.parquet") as snk:
        snk.process(entity(src))
```

### Document generation

```python
from dkit.doc2.document import Document
from dkit.doc2.html_renderer import HtmlRenderer

doc = Document()
doc.add_template("# Hello\n\nThis is a **report**.")

renderer = HtmlRenderer(css="email.css")
renderer.render(doc, "report.html")

# inline CSS for email clients
html = renderer.render_email_string(doc)
```

---

## CLI — `dk`

The `dk` command-line tool exposes the library for interactive and scripted
data processing. Run `dk MODULE -h` for full options on any module.

```
dk MODULE [subcommand] [options]
```

### Maintenance modules

| Module | Purpose |
|---|---|
| `dk schemas` | Manage entity schemas — infer, list, export, reflect from SQL/HDF5 |
| `dk connections` | Manage database connections |
| `dk endpoints` | Manage data endpoints |
| `dk queries` | Manage SQL queries |
| `dk transforms` | Manage field transform rules |
| `dk mapping` | Manage entity-relation mappings |
| `dk admin` | Configuration administration |
| `dk styles` | Register and preview document style packs |

### Action modules

| Module | Purpose |
|---|---|
| `dk run` | Run ETL pipelines, queries, and aggregations |
| `dk xplore` | Explore and summarise data interactively |
| `dk diff` | Find differences between two datasets |
| `dk build` | Build documents from Markdown sources |

### Data exploration (`dk xplore`)

```bash
dk xplore head        data.jsonl           # first n rows
dk xplore summary     data.jsonl           # field statistics
dk xplore histogram   data.jsonl -f price  # histogram for a field
dk xplore count       data.jsonl -f status # count by field value
dk xplore distinct    data.jsonl -f country
dk xplore duplicates  data.jsonl -k id
dk xplore view        data.jsonl           # interactive grid
dk xplore table       data.jsonl           # tabular output
dk xplore fields      data.jsonl           # list field names
dk xplore struc       data.jsonl           # structure map
dk xplore plot        data.jsonl           # plot grammar
dk xplore sample      database.db          # sample from DB tables
```

### Schema operations (`dk schemas`)

```bash
dk schemas infer      data.csv             # infer schema from data
dk schemas ls                              # list stored schemas
dk schemas print      person               # print a schema
dk schemas export     person --format arrow
dk schemas sql-reflect mydb --connection sqlite
dk schemas grep       "person.*"
```

### Running pipelines (`dk run`)

| Subcommand | Purpose |
|---|---|
| `etl` | Run an ETL process (format conversion with optional transforms) |
| `agg` | Group-by aggregation |
| `join` | Join two datasets |
| `pivot` | Create a pivot table |
| `melt` | Transpose a pivot table back to key:value pairs |
| `query` | Execute a SQL query |
| `exec` | Execute a driver-level query (DDL etc.) |
| `template` | Apply datasets to a Jinja2 template |

```bash
dk run etl      input.csv  -o output.parquet
dk run agg      input.jsonl -o output.jsonl
dk run join     left.jsonl right.jsonl -o result.jsonl
dk run pivot    input.jsonl -o pivot.jsonl
dk run melt     pivot.jsonl -o output.jsonl
dk run query    --connection mydb "select * from person"
dk run exec     --connection mydb "create index ..."
dk run template template.j2 data.jsonl
```

Document reports are run via `dk build report report.yml` -- all
document/report functionality lives under `dk build`, not `dk run`.

### Document styles

Install a style-pack distribution and register it once in the DK configuration:

```bash
dk styles register dkit-blue --distribution libdkit \
  --manifest dkit/stylepacks/dkit_blue/style.yaml
dk styles validate dkit-blue
dk build doc --style dkit-blue --format reportlab \
  --title "Quarterly report" --output report.pdf report.md
dk styles preview dkit-blue --output style-preview
```

Style packs contain native CSS, DOCX, LaTeX, ReportLab, and matplotlib
resources plus a shared manifest. Registration records a fingerprint and
does not execute provider code. Only install style wheels from sources you
trust: a style pack can contain LaTeX code, templates, fonts, images, and
other resources that are consumed by the relevant document tools.

---

## Development

```bash
make test      # run test suite with coverage
make build     # build sdist and wheel
make install   # install into current environment
make clean     # remove build artefacts
```

Tests live in `test/`. Run from the repo root:

```bash
pytest               # all tests
pytest --cov         # with coverage report
```

Integration tests that require external services (HDFS, SFTP, ZMQ) are
prefixed with `_test_` and excluded from the default run.

---

## Project links

- Source: https://github.com/cobusn/dkit
- Issues: https://github.com/cobusn/dkit/issues
