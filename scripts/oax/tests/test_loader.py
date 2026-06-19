"""Regression tests for the OpenAPI spec loader."""

from pathlib import Path

import oax


def test_load_spec_handles_admin_interface_json():
    """Load the AdminInterface spec without raising serialization errors."""
    spec_path = Path(__file__).resolve().parents[1] / "AdminInterface.json"

    spec = oax.load_spec(str(spec_path))

    assert spec["openapi"] == "3.0.2"
    assert len(spec["paths"]) > 0
