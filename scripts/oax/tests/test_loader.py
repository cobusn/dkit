"""Regression tests for the OpenAPI spec loader."""

import subprocess
import sys
from pathlib import Path

import oax


def test_load_spec_handles_admin_interface_json():
    """Load the AdminInterface spec without raising serialization errors."""
    spec_path = Path(__file__).resolve().parents[1] / "AdminInterface.json"

    spec = oax.load_spec(str(spec_path))

    assert spec["openapi"] == "3.0.2"
    assert len(spec["paths"]) > 0


def test_cli_version_flag_reports_module_version():
    """Expose the module version through the command-line interface."""
    script_path = Path(__file__).resolve().parents[1] / "oax.py"

    result = subprocess.run(
        [sys.executable, str(script_path), "--version"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == f"oax {oax.__version__}"
