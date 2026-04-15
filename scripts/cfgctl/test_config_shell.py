import pytest
from cryptography.fernet import Fernet
from config_shell import ConfigShell, load_file


SAMPLE_CONFIG = """\
unix:
  defaults:
    source: AWK2
    enabled: true
  uri: https://example.test/unix
aws:
  secret: key3
items:
  - first
  - second
"""


def make_shell(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text(SAMPLE_CONFIG)
    return ConfigShell(path), path


def test_cd_and_pwd_follow_current_node(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_cd("unix/defaults")
    shell.do_pwd("")

    assert shell.cwd == ["unix", "defaults"]
    assert capsys.readouterr().out.strip() == "/unix/defaults"


def test_get_set_and_rm_are_relative_to_current_node(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)
    shell.do_cd("unix/defaults")

    shell.do_get("source")
    assert capsys.readouterr().out.strip() == "AWK2"

    shell.do_set("enabled false")
    assert shell._get_value("enabled") is False
    assert shell.prompt.startswith("cfg*:")
    capsys.readouterr()

    shell.do_rm("enabled")
    assert "enabled" not in shell._get_value("")


def test_set_keeps_quoted_numbers_as_strings(tmp_path):
    shell, _ = make_shell(tmp_path)

    shell.do_set('unix/defaults/source "123"')
    assert shell._get_value("unix/defaults/source") == "123"
    assert isinstance(shell._get_value("unix/defaults/source"), str)

    shell.do_set("unix/defaults/source 123")
    assert shell._get_value("unix/defaults/source") == 123
    assert isinstance(shell._get_value("unix/defaults/source"), int)


def test_set_coerces_negative_integers(tmp_path):
    shell, _ = make_shell(tmp_path)

    shell.do_set("unix/defaults/source -5")
    assert shell._get_value("unix/defaults/source") == -5
    assert isinstance(shell._get_value("unix/defaults/source"), int)

    shell.do_set("unix/defaults/source -1.5")
    assert shell._get_value("unix/defaults/source") == -1.5
    assert isinstance(shell._get_value("unix/defaults/source"), float)

    shell.do_set("unix/defaults/source -abc")
    assert shell._get_value("unix/defaults/source") == "-abc"
    assert isinstance(shell._get_value("unix/defaults/source"), str)


def test_set_coerces_unquoted_floats(tmp_path):
    shell, _ = make_shell(tmp_path)

    shell.do_set("unix/defaults/source 1.25")
    assert shell._get_value("unix/defaults/source") == 1.25
    assert isinstance(shell._get_value("unix/defaults/source"), float)

    shell.do_set("unix/defaults/source 1e3")
    assert shell._get_value("unix/defaults/source") == 1000.0
    assert isinstance(shell._get_value("unix/defaults/source"), float)

    shell.do_set('unix/defaults/source "1.25"')
    assert shell._get_value("unix/defaults/source") == "1.25"
    assert isinstance(shell._get_value("unix/defaults/source"), str)


def test_ls_show_and_tree_render_expected_output(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_ls("")
    ls_out = capsys.readouterr().out
    assert "unix/" in ls_out
    assert "aws/" in ls_out
    assert "items/" in ls_out

    shell.do_show("unix")
    show_out = capsys.readouterr().out
    assert "defaults" in show_out
    assert "uri" in show_out
    assert "<dict>" in show_out

    shell.do_tree("unix")
    tree_out = capsys.readouterr().out
    assert tree_out.startswith("unix")
    assert "├── defaults/" in tree_out
    assert "└── uri" in tree_out


def test_save_and_reload_round_trip_changes(tmp_path):
    shell, path = make_shell(tmp_path)

    shell.do_set("unix/defaults/source Updated")
    assert shell.dirty is True
    shell.do_save("")
    assert shell.dirty is False

    saved = load_file(path)
    assert saved["unix"]["defaults"]["source"] == "Updated"

    path.write_text(SAMPLE_CONFIG.replace("AWK2", "Reloaded"))
    shell.do_reload("")
    assert shell._get_value("unix/defaults/source") == "Reloaded"
    assert shell.cwd == []


def test_save_can_write_to_an_alternate_filename(tmp_path):
    shell, path = make_shell(tmp_path)
    target = tmp_path / "copy.yml"

    shell.do_set("unix/defaults/source Updated")
    shell.do_save(str(target))

    assert load_file(target)["unix"]["defaults"]["source"] == "Updated"
    assert shell.path == str(target)
    assert load_file(path)["unix"]["defaults"]["source"] == "AWK2"


def test_exit_warns_when_file_is_dirty(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_set("unix/defaults/source Updated")
    capsys.readouterr()

    assert shell.do_exit("") is False
    assert "File changed, still exit?" in capsys.readouterr().out
    assert shell.do_exit("yes") is True


def test_completion_supports_nested_paths(tmp_path):
    shell, _ = make_shell(tmp_path)

    assert shell.complete_cd("u", "cd u", 3, 4) == ["unix/"]
    assert shell.complete_cd("unix/", "cd unix/", 3, 9) == ["unix/defaults/"]
    assert shell.complete_get("unix/", "get unix/", 4, 10) == [
        "unix/defaults/",
        "unix/uri",
    ]
    assert shell.complete_yank("unix/", "yank unix/", 5, 11) == [
        "unix/defaults/",
        "unix/uri",
    ]


def test_mkdir_creates_empty_dict(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_mkdir("unix/new_section")
    assert shell._get_value("unix/new_section") == {}
    assert shell.dirty is True
    assert "Created" in capsys.readouterr().out

    # can immediately cd into it
    shell.do_cd("unix/new_section")
    assert shell.cwd == ["unix", "new_section"]


def test_mkdir_creates_at_root_level(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_mkdir("new_top")
    assert shell._get_value("new_top") == {}
    capsys.readouterr()


def test_mkdir_rejects_existing_path(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_mkdir("unix")
    assert "Already exists" in capsys.readouterr().out
    assert not shell.dirty


def test_mkdir_rejects_missing_parent(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    shell.do_mkdir("nonexistent/new_section")
    assert "Key not found" in capsys.readouterr().out
    assert not shell.dirty


def test_invalid_paths_print_key_not_found(tmp_path, capsys):
    shell, _ = make_shell(tmp_path)

    # non-integer index on a list
    shell.do_cd("items/foo")
    assert "Key not found" in capsys.readouterr().out

    # navigating past a scalar
    shell.do_cd("unix/defaults/source/nested")
    assert "Key not found" in capsys.readouterr().out

    shell.do_get("items/foo")
    assert "Key not found" in capsys.readouterr().out

    shell.do_get("unix/defaults/source/nested")
    assert "Key not found" in capsys.readouterr().out


def test_encode_raises_on_invalid_token(tmp_path):
    shell, _ = make_shell(tmp_path)
    shell.token = "not-a-valid-token"
    with pytest.raises(ValueError, match="Invalid token provided"):
        shell._encode("secret")


def test_encode_raises_on_non_string_value(tmp_path):
    shell, _ = make_shell(tmp_path)
    shell.token = Fernet.generate_key()
    with pytest.raises(ValueError, match="Cannot encode non-string value"):
        shell._encode(123)


def test_encode_roundtrip(tmp_path):
    shell, _ = make_shell(tmp_path)
    key = Fernet.generate_key()
    shell.token = key
    encrypted = shell._encode("secret")
    assert isinstance(encrypted, str)
    assert Fernet(key).decrypt(encrypted.encode()).decode() == "secret"


def test_yank_copies_serialized_value(tmp_path, capsys, monkeypatch):
    shell, _ = make_shell(tmp_path)
    captured = {}

    def fake_copy(value):
        captured["value"] = value

    monkeypatch.setattr("config_shell.pyperclip.copy", fake_copy)

    shell.do_yank("unix/defaults")

    assert "source: AWK2" in captured["value"]
    assert "enabled: true" in captured["value"]
    assert capsys.readouterr().out.strip() == "Copied unix/defaults"
