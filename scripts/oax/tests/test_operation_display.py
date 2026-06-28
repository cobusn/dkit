"""Regression tests for operation rendering."""

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import oax


def _make_shell():
    shell = oax.OAX()
    spec_path = Path(__file__).resolve().parents[1] / "AdminInterface.json"
    shell.spec = oax.load_spec(str(spec_path))
    shell.source = "AdminInterface.json"
    shell.opt_color = False
    shell.opt_verbose = False
    return shell


def _capture_output(callback):
    buffer = StringIO()
    original_console = oax.console
    if oax.RICH:
        oax.console = oax.Console(file=buffer, force_terminal=False, width=120)
    try:
        with redirect_stdout(buffer):
            callback()
    finally:
        oax.console = original_console
    return buffer.getvalue()


def test_op_renders_request_body_schema():
    """Show request-body schemas inline for operations that use them."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_op("/BillingSession/get_active_sessions_list post")
    )

    assert "Request Body" in output
    assert "Schema: GetActiveSessionsRequest" in output
    assert "account_id" not in output
    assert "Response 200 body" not in output
    assert "ArrayOfActiveSessionInfo" not in output


def test_params_renders_request_body_schema_when_no_operation_params():
    """Show request-body fields for operations that omit parameters."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_params("/BillingSession/get_active_sessions_list post")
    )

    assert "Request Body" in output
    assert "GetActiveSessionsRequest" in output
    assert "account_id" in output


def test_params_renders_schema_details_for_standard_parameters():
    """Show schema details such as format and example in params output."""
    shell = _make_shell()
    shell.do_set("details on")

    output = _capture_output(
        lambda: shell.do_params("/TraceSession/get_log_info post")
    )

    assert "from_date" in output
    assert "format=date-time" in output
    assert "example='2000-01-01" in output
    assert "08:00:00'" in output
    assert "merged_session_id_list" in output


def test_response_renders_referenced_schema_name():
    """Show referenced schema names for response bodies."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_response(
            "/BillingSession/get_active_sessions_list post 200"
        )
    )

    assert "ArrayOfActiveSessionInfo" in output
    assert "active_session_list" in output
    assert "total" in output


def test_tag_accepts_wrapped_quotes():
    """Allow quoted tag names with spaces and punctuation."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_tag('"ACL | ACL management"')
    )

    assert "Get the list of ACL" in output


def test_search_uses_raw_spec_for_schema_metadata():
    """Search schema metadata without traversing resolved reference cycles."""
    shell = _make_shell()

    output = _capture_output(lambda: shell.do_search("meta_info"))

    assert 'Search: "meta_info"' in output
    assert "schema" in output
    assert "SessionLogMessage" in output


def test_load_caches_raw_spec(monkeypatch):
    """Reuse the raw spec from load instead of reparsing it later."""
    shell = oax.OAX()
    spec_path = Path(__file__).resolve().parents[1] / "AdminInterface.json"

    _capture_output(lambda: shell.do_load(str(spec_path)))
    assert shell.raw_spec is not None

    calls = {"count": 0}

    def _unexpected_load(*args, **kwargs):
        calls["count"] += 1
        raise AssertionError("_load_raw should not be called after load")

    monkeypatch.setattr(oax, "_load_raw", _unexpected_load)

    output = _capture_output(
        lambda: shell.do_op("/BillingSession/get_active_sessions_list post")
    )

    assert "GetActiveSessionsRequest" in output
    assert calls["count"] == 0


def test_response_renders_compact_array_summary():
    """Show a compact response summary without deep item expansion."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_response("/ACL/get_acl_list post 200")
    )

    assert "ArrayOfAclInfo" in output
    assert "acl_list" in output
    assert "items" not in output
    assert "group" not in output


def test_body_flattens_trivial_array_composition():
    """Keep request-body fields compact while showing details when enabled."""
    shell = _make_shell()
    shell.do_set("details on")

    output = _capture_output(
        lambda: shell.do_body("/TraceSession/get_log_info post")
    )

    assert "merged_session_id_list" in output
    assert "session_id" in output
    assert "items" in output
    assert "allOf[1]" not in output
    assert "Details" in output
    assert "format=date-time" in output
    assert "example='2000-01-01 08:00:00'" in output


def test_example_generates_synthesized_schema():
    """Generate a compact JSON example for a component schema."""
    shell = _make_shell()

    output = _capture_output(
        lambda: shell.do_example("GetSessionLogInfoRequest")
    )

    assert "Example: GetSessionLogInfoRequest" in output
    assert '"session_id": "string"' in output
    assert '"from_date": "2000-01-01 08:00:00"' in output
    assert '"to_date": "2000-01-01 08:00:00"' in output


def test_model_exports_schema_with_example_and_file(tmp_path):
    """Export a nested Pydantic model and write it to disk."""
    shell = _make_shell()
    out_path = tmp_path / "session_log_request.py"

    output = _capture_output(
        lambda: shell.do_model(
            f'GetSessionLogInfoRequest --example --file "{out_path}"'
        )
    )

    assert f"wrote model to: {out_path}" in output
    rendered = out_path.read_text(encoding="utf-8")
    assert "class GetSessionLogInfoRequest(BaseModel):" in rendered
    assert "class GetSessionLogInfoRequestMergedSessionIdList(BaseModel):" in rendered
    assert "merged_session_id_list: Optional[List[GetSessionLogInfoRequestMergedSessionIdList]] = None" in rendered
    assert "GetSessionLogInfoRequest_EXAMPLE = GetSessionLogInfoRequest.model_validate(" in rendered


def test_model_copies_schema_to_clipboard(monkeypatch):
    """Copy a rendered model to the clipboard when requested."""
    shell = _make_shell()
    copied = {"value": None}

    class _Clipboard:
        @staticmethod
        def copy(text):
            copied["value"] = text

    monkeypatch.setattr(oax, "pyperclip", _Clipboard)

    output = _capture_output(
        lambda: shell.do_model("GetSessionLogInfoRequest --copy")
    )

    assert "model copied to clipboard" in output
    assert copied["value"] is not None
    assert "class GetSessionLogInfoRequest(BaseModel):" in copied["value"]


def test_model_copies_example_only_to_clipboard(monkeypatch):
    """Copy only the populated example when example-only JSON is requested."""
    shell = _make_shell()
    copied = {"value": None}

    class _Clipboard:
        @staticmethod
        def copy(text):
            copied["value"] = text

    monkeypatch.setattr(oax, "pyperclip", _Clipboard)

    output = _capture_output(
        lambda: shell.do_model("GetSessionLogInfoRequest --example-only --json --copy")
    )

    assert "example copied to clipboard" in output
    assert copied["value"] is not None
    assert "class GetSessionLogInfoRequest(BaseModel):" not in copied["value"]
    assert copied["value"].lstrip().startswith("{")
    assert '"session_id": "string"' in copied["value"]
    assert '"from_date": "2000-01-01 08:00:00"' in copied["value"]
    assert '"to_date": "2000-01-01 08:00:00"' in copied["value"]


def test_schema_uses_compact_details_rendering():
    """Render schemas compactly by default and expand one level with details."""
    shell = _make_shell()

    compact = _capture_output(
        lambda: shell.do_schema("GetSessionLogInfoRequest")
    )
    assert "from_date" in compact
    assert "items" not in compact
    assert "Details" not in compact

    shell.do_set("details on")
    detailed = _capture_output(
        lambda: shell.do_schema("GetSessionLogInfoRequest")
    )
    assert "from_date" in detailed
    assert "Details" in detailed
    assert "format=date-time" in detailed
    assert "items" in detailed
