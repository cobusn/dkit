# Doc2 style-pack examples

This folder provides three small, runnable scenarios for verifying an
installed document style pack:

1. `api_build.py` builds a canonical document directly through the Python API.
2. `document.md` is built through `dk build doc` with an explicit `--style`.
3. `report_project/` is a complete `dk build report` project. Its Python
   content creates both a Doc2 table and a matplotlib chart.

Register a style before running an individual scenario. For the bundled
reference pack:

```bash
dk styles register dkit-blue --distribution libdkit \
  --manifest dkit/stylepacks/dkit_blue/style.yaml
```

## Individual scenarios

From this folder:

```bash
python api_build.py dkit-blue --format reportlab --output output/api.pdf

dk build doc --style dkit-blue --format html \
  --title "Markdown document" --output output/document.html document.md

cd report_project
dk build report --report report.yaml
```

The project scenario defaults to `dkit-blue`; change
`configuration.style` in `report_project/report.yaml` to use another
registered style.

## Build every scenario

`build_all.py` resolves one registered style, creates private registration
state for its child CLI processes, and writes all primary outputs into a
single folder without changing the example sources:

```bash
python build_all.py dkit-blue --output output
```

For example, this produces `dkit_blue_api_rl.pdf`,
`dkit_blue_doc_latex.pdf`, and `dkit_blue_report_docx.docx`. It also writes:

- `dkit_blue_email.html`: rendered email HTML with the style's email CSS
  inlined.
- `dkit_blue_email.eml`: a complete MIME message with plain-text fallback,
  inlined HTML, and CID-attached chart image.

No mail is sent.
