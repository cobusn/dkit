# Executive summary

This project is built with `dk build report`. The style is selected in
`report.yaml`, rather than supplied through a one-shot CLI option.

## Delivery status

The following table is generated through the Doc2 API by
`src.summary.Summary.table()` and inserted into this Markdown template.

{{ summary.table() }}

## Completion trend

The chart is created by project Python code inside the selected matplotlib
theme context.

{{ summary.chart() }}

## Completion profile

{{ summary.line_chart() }}

## Completion observations

{{ summary.scatter_chart() }}

## Completion by service

{{ summary.treemap() }}
