"""Tests for the M2 style registry and styles CLI."""

from pathlib import Path
import shutil

import pytest

from dkit.stylepack.config import StyleConfigError, StyleConfigRepository
from dkit.stylepack.fingerprint import fingerprint_root
from dkit.stylepack.loader import StylePackLoader
from dkit.stylepack.model import StylePackReference
from dkit.stylepack.registry import StyleRegistry
from lib_dk import styles_module


FIXTURE_ROOT = Path(__file__).parent / "data" / "stylepack"


class FixtureDistribution:
    """Distribution-like object for the declarative fixture."""

    version = "0.1.0"

    def __init__(self, root=None):
        self.root = root or FIXTURE_ROOT

    def locate_file(self, relative_path):
        return self.root / relative_path


def make_reference():
    """Build a reference for the fixture distribution."""
    return StylePackReference(
        name="dkit-blue",
        distribution="test-style-pack",
        manifest="blue/style.yaml",
    )


def make_registry(config_path, distribution=None):
    """Build a registry using the fixture distribution."""
    distribution = distribution or FixtureDistribution()
    loader = StylePackLoader(lambda _name: distribution)
    return StyleRegistry(config_path=config_path, loader=loader)


def test_repository_reads_registered_reference_without_default_options(tmp_path):
    """Style sections do not accidentally inherit unrelated defaults."""
    config_path = tmp_path / "dk.ini"
    config_path.write_text(
        "[DEFAULT]\nkey = preserved\n\n"
        "[style:dkit-blue]\n"
        "distribution = test-style-pack\n"
        "manifest = blue/style.yaml\n"
        "fingerprint = sha256:fixture\n",
        encoding="utf-8",
    )

    reference = StyleConfigRepository(config_path).get("DKIT-BLUE")

    assert reference.distribution == "test-style-pack"
    assert reference.manifest == "blue/style.yaml"


def test_repository_preserves_unrelated_ini_content(tmp_path):
    """Adding a registration preserves existing sections and comments."""
    config_path = tmp_path / "dk.ini"
    original = "# keep this comment\n[DOC]\nstyle = default\n"
    config_path.write_text(original, encoding="utf-8")
    reference = make_reference().model_copy(
        update={"fingerprint": "sha256:fixture"}
    )

    StyleConfigRepository(config_path).set(reference)
    content = config_path.read_text(encoding="utf-8")

    assert content.startswith(original)
    assert "[style:dkit-blue]" in content
    assert "style = default" in content


def test_repository_replaces_only_the_named_section(tmp_path):
    """Replacing a style leaves later INI sections untouched."""
    config_path = tmp_path / "dk.ini"
    config_path.write_text(
        "[style:dkit-blue]\n"
        "distribution = old\n"
        "manifest = old/style.yaml\n"
        "\n[other]\nvalue = keep\n",
        encoding="utf-8",
    )
    reference = make_reference().model_copy(
        update={"fingerprint": "sha256:new"}
    )

    StyleConfigRepository(config_path).set(reference, replace_existing=True)
    content = config_path.read_text(encoding="utf-8")

    assert "distribution = old" not in content
    assert "manifest = blue/style.yaml" in content
    assert "[other]\nvalue = keep\n" in content


def test_repository_refuses_duplicate_without_replace(tmp_path):
    """Accidental replacement requires explicit confirmation."""
    repository = StyleConfigRepository(tmp_path / "dk.ini")
    reference = make_reference()
    repository.set(reference)

    with pytest.raises(StyleConfigError, match="already registered"):
        repository.set(reference)


def test_repository_refuses_removing_the_configured_default(tmp_path):
    """The configured default cannot become dangling by accident."""
    config_path = tmp_path / "dk.ini"
    config_path.write_text("[DOC]\nstyle = dkit-blue\n", encoding="utf-8")
    repository = StyleConfigRepository(config_path)
    repository.set(make_reference())

    with pytest.raises(StyleConfigError, match="configured DOC default"):
        repository.remove("dkit-blue")

    repository.remove("dkit-blue", force=True)
    assert "style:dkit-blue" not in config_path.read_text(encoding="utf-8")


def test_registry_persists_verified_fingerprint(tmp_path):
    """Registry registration validates first and stores the current digest."""
    registry = make_registry(tmp_path / "dk.ini")

    persisted = registry.register(make_reference())

    assert persisted.fingerprint == fingerprint_root(FIXTURE_ROOT / "blue")
    assert registry.reference("dkit-blue") == persisted


def test_registry_refreshes_fingerprint_after_style_update(tmp_path):
    """Refreshing trusts the current installed style resources."""
    root = tmp_path / "style-pack"
    shutil.copytree(FIXTURE_ROOT, root)
    distribution = FixtureDistribution(root)
    registry = make_registry(tmp_path / "dk.ini", distribution)
    registered = registry.register(make_reference())

    manifest = root / "blue" / "style.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8") + "\n# updated\n",
        encoding="utf-8",
    )

    refreshed = registry.refresh("dkit-blue")

    assert refreshed.fingerprint != registered.fingerprint
    assert registry.get("dkit-blue").manifest.id == "dkit-blue"


def test_registry_lists_sorted_names(tmp_path):
    """The registry provides deterministic names for the CLI."""
    registry = make_registry(tmp_path / "dk.ini")
    registry.register(make_reference())
    second = make_reference().model_copy(
        update={"name": "another", "fingerprint": "sha256:fixture"}
    )
    repository = registry.repository
    repository.set(second)

    assert registry.names() == ["another", "dkit-blue"]


def test_styles_cli_dispatches_and_prints_registration(tmp_path, monkeypatch, capsys):
    """The styles module exposes registration through the application CLI."""
    config_path = tmp_path / "dk.ini"
    registry = make_registry(config_path)
    monkeypatch.setattr(styles_module, "StyleRegistry", lambda _path: registry)

    styles_module.StylesModule([
        "register",
        "dkit-blue",
        "--distribution",
        "test-style-pack",
        "--manifest",
        "blue/style.yaml",
        "--config",
        str(config_path),
    ]).run()

    assert "registered style 'dkit-blue'" in capsys.readouterr().out


def test_styles_cli_refreshes_registration(tmp_path, monkeypatch, capsys):
    """The styles CLI exposes fingerprint refresh."""
    registry = make_registry(tmp_path / "dk.ini")
    registry.register(make_reference())
    monkeypatch.setattr(styles_module, "StyleRegistry", lambda _path: registry)

    styles_module.StylesModule([
        "refresh",
        "dkit-blue",
        "--config",
        str(tmp_path / "dk.ini"),
    ]).run()

    assert "refreshed style 'dkit-blue'" in capsys.readouterr().out


def test_styles_cli_errors_use_stderr_and_nonzero_exit(tmp_path, capsys):
    """Missing styles fail through the CLI error boundary."""
    with pytest.raises(SystemExit) as caught:
        styles_module.StylesModule([
            "validate", "missing", "--config", str(tmp_path / "dk.ini")
        ]).run()

    captured = capsys.readouterr()
    assert caught.value.code == 1
    assert "unknown style 'missing'" in captured.err
    assert captured.out == ""
