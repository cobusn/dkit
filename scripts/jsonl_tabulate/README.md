# jsonl-tabulate

Tabulate JSONL input from `stdin` or files.

## Install

```bash
python -m pip install .
```

## Usage

```bash
cat data.jsonl | jsonl-tabulate
jsonl-tabulate data.jsonl
jsonl-tabulate --format github data.jsonl
```

## Test

```bash
make test
```

