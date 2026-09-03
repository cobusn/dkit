"""Tests for the jsonl-tabulate CLI helpers."""

from jsonl_table.jsonl_tabulate import _collect_headers, _load_jsonl, _rows_to_table


def test_collect_headers_preserves_first_seen_order():
    rows = [{"a": 1, "b": 2}, {"b": 3, "c": 4}]

    assert _collect_headers(rows) == ["a", "b", "c"]


def test_collect_headers_can_sort_keys():
    rows = [{"b": 2, "a": 1}]

    assert _collect_headers(rows, sort_keys=True) == ["a", "b"]


def test_load_jsonl_wraps_scalar_values():
    rows = _load_jsonl(['{"a": 1}\n', '42\n'])

    assert rows == [{"a": 1}, {"value": 42}]


def test_rows_to_table_fills_missing_values():
    rows = [{"a": 1}, {"b": 2}]

    assert _rows_to_table(rows, ["a", "b"]) == [[1, ""], ["", 2]]
