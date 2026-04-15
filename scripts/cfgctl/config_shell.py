# Copyright (c) 2026 Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Interactive shell utilities for editing JSON and YAML configuration files.

This module provides file load/save helpers and the ``ConfigShell`` command
interpreter used by ``cfgctl``. The shell exposes nested configuration data as
filesystem-like paths so users can inspect, navigate, modify, encrypt, and save
document content from an interactive prompt or through command dispatch.
"""
import json
import yaml
import shlex
from cmd import Cmd
from pathlib import Path
import pyperclip
from tabulate import tabulate
from cryptography.fernet import Fernet

try:
    import readline
except ImportError:
    readline = None

__version__ = "26.4.1"
__all__ = [
    "__version__",
    "ConfigShell",
]


def load_file(path):
    """Load a JSON or YAML document from disk."""
    path = Path(path)
    with open(path) as f:
        if path.suffix in (".yaml", ".yml"):
            return yaml.safe_load(f) or {}
        return json.load(f)


def save_file(path, data):
    """Write a JSON or YAML document to disk."""
    path = Path(path)
    with open(path, "w") as f:
        if path.suffix in (".yaml", ".yml"):
            yaml.safe_dump(data, f, sort_keys=False)
        else:
            json.dump(data, f, indent=2)


def coerce_value(value, quoted=False):
    """Coerce an unquoted string to a simple scalar type when possible."""
    if quoted:
        return value[1:-1]

    try:
        return int(value)
    except ValueError:
        pass

    try:
        if any(char in value for char in (".", "e", "E")):
            return float(value)
    except ValueError:
        pass

    lowered = value.lower()
    if lowered in ("true", "false"):
        return lowered == "true"
    if lowered == "null":
        return None
    return value


class ConfigShell(Cmd):
    """Interactive filesystem-style shell for editing JSON and YAML documents.

    The shell treats nested dictionaries and lists like directories and paths,
    with commands such as ``cd``, ``ls``, ``show``, ``tree``, ``set``, and
    ``rm`` operating relative to the current node.

    Note:
        Constructing this class modifies global ``readline`` completion
        delimiters, when ``readline`` is available, so ``/`` is treated as part
        of a completion token.
    """
    intro = "Config CLI. Type help or ? to list commands."
    prompt = "cfg:/ > "

    def __init__(self, path:str, token: str=None):
        super().__init__()
        self.path = path
        self.raw = load_file(path)
        self.cwd = []
        self.dirty = False
        self.token = token
        if readline is not None:
            delims = readline.get_completer_delims().replace("/", "")
            readline.set_completer_delims(delims)
        self._update_prompt()

    def _cwd_display(self):
        """Return the current working path in shell form."""
        return "/" if not self.cwd else "/" + "/".join(self.cwd)

    def _update_prompt(self):
        """Refresh the prompt to reflect the current working path."""
        marker = "*" if self.dirty else ""
        self.prompt = f"cfg{marker}:{self._cwd_display()} > "

    def _split_path(self, path):
        """Split a filesystem-style path into meaningful segments."""
        if not path:
            return []
        return [part for part in path.split("/") if part not in ("", ".")]

    def _resolve_path(self, path):
        """Resolve an absolute or relative path against the current node."""
        if not path:
            return list(self.cwd)

        if path.startswith("/"):
            parts = []
        else:
            parts = list(self.cwd)

        for part in self._split_path(path):
            if part == "..":
                if parts:
                    parts.pop()
            else:
                parts.append(part)
        return parts

    def _coerce_key(self, part, value):
        """Convert a path segment to a key or index for the current container."""
        if isinstance(value, list):
            return int(part)
        return part

    def _walk(self, parts):
        """Return the node located at the resolved path parts."""
        value = self.raw
        for part in parts:
            try:
                key = self._coerce_key(part, value)
                value = value[key]
            except (TypeError, ValueError, IndexError):
                raise KeyError(part)
        return value

    def _resolve_value(self, path=""):
        """Resolve a path and return both its parts and current value."""
        parts = self._resolve_path(path)
        value = self.raw if not parts else self._walk(parts)
        return parts, value

    def _resolve_parent(self, path):
        """Resolve a path and return its parent container and final key."""
        parts = self._resolve_path(path)
        if not parts:
            raise KeyError("Root has no parent")
        parent = self.raw if len(parts) == 1 else self._walk(parts[:-1])
        return parts, parent, self._coerce_key(parts[-1], parent)

    def _render_value(self, value):
        """Render a node summary for table/list output."""
        if isinstance(value, dict):
            return "<dict>"
        if isinstance(value, list):
            return f"<list:{len(value)}>"
        return value

    def _serialize_value(self, value):
        """Serialize a node for clipboard-friendly output."""
        if isinstance(value, (dict, list)):
            return yaml.safe_dump(value, sort_keys=False).rstrip()
        if value is None:
            return "null"
        return str(value)

    def _iter_children(self, value):
        """Return immediate children for a dict or list node."""
        if isinstance(value, dict):
            return [(str(key), child) for key, child in value.items()]
        if isinstance(value, list):
            return [(str(i), child) for i, child in enumerate(value)]
        return []

    def _get_value(self, path=""):
        """Return the value at a path, defaulting to the current node."""
        return self._resolve_value(path)[1]

    def _set_value(self, path, value):
        """Set the value at a resolved path."""
        parts, parent, key = self._resolve_parent(path)
        if not parts:
            raise KeyError("Cannot replace root directly")
        parent[key] = value
        self.dirty = True
        self._update_prompt()

    def _del_value(self, path):
        """Delete the value at a resolved path."""
        parts, parent, key = self._resolve_parent(path)
        if not parts:
            raise KeyError("Cannot remove root")
        del parent[key]
        self.dirty = True
        self._update_prompt()

    # -------- Core Commands --------

    def do_ls(self, arg):
        """List entries in the current node or a target path: ls [path]"""
        try:
            value = self._get_value(arg.strip())
        except KeyError:
            print("Key not found")
            return

        children = self._iter_children(value)
        if children:
            for key, child in children:
                suffix = "/" if isinstance(child, (dict, list)) else ""
                print(f"{key}{suffix}")
        else:
            print(value)

    def do_get(self, arg):
        """Get a value from the current node or a target path: get [path]"""
        try:
            print(self._get_value(arg.strip()))
        except KeyError:
            print("Key not found")

    def do_show(self, arg):
        """Show immediate keys and values in a table: show [path]"""
        try:
            value = self._get_value(arg.strip())
        except KeyError:
            print("Key not found")
            return

        rows = []
        children = self._iter_children(value)
        if children:
            for key, child in children:
                rows.append([key, self._render_value(child)])
        else:
            rows.append(["value", value])

        print(tabulate(rows, headers=["key", "value"]))

    def do_tree(self, arg):
        """Display a Unix-style tree for the current node or a target path."""
        target = arg.strip()
        try:
            value = self._get_value(target)
        except KeyError:
            print("Key not found")
            return

        parts = self._resolve_path(target)
        root_name = "/" if not parts else parts[-1]
        print(root_name)
        for line in self._tree_lines(value):
            print(line)

    def do_yank(self, arg):
        """Copy a value to the system clipboard: yank [path]"""
        target = arg.strip()
        try:
            value = self._get_value(target)
        except KeyError:
            print("Key not found")
            return

        try:
            pyperclip.copy(self._serialize_value(value))
        except pyperclip.PyperclipException as e:
            print(f"Clipboard error: {e}")
            return

        label = target or self._cwd_display()
        print(f"Copied {label}")

    def _encode(self, value: str):
        if not isinstance(value, str):
            raise ValueError(f"Cannot encode non-string value: {type(value).__name__}")
        try:
            fernet = Fernet(self.token)
        except (TypeError, ValueError):
            raise ValueError("Invalid token provided")
        return fernet.encrypt(value.encode()).decode()

    def do_encode(self, arg):
        """Encode value at a specified path"""
        args = shlex.split(arg, posix=False)
        try:
            if len(args) == 1:
                path = args[0]
                self._set_value(path, self._encode(self._get_value(path)))
            elif len(args) == 2:
                path, value = args
                self._set_value(path, self._encode(value))
            else:
                raise TypeError("Usage: encode <path> [value]")
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error: {e}")

    def do_set(self, arg):
        """Set a value at a path: set <path> <value>

        Coercion rules for unquoted values:
        - digits become integers, for example ``123``
        - decimal or scientific notation becomes float, for example ``1.25`` or ``1e6``
        - ``true`` and ``false`` become booleans
        - ``null`` becomes ``None``

        Quoted values remain strings, even if they look like numbers or
        booleans. For example:
        - ``set service/count 123`` stores the integer ``123``
        - ``set service/count "123"`` stores the string ``"123"``
        """
        try:
            args = shlex.split(arg, posix=False)
            if len(args) < 2:
                print("Usage: set <key> <value>")
                return

            key, value = args[0], " ".join(args[1:])
            quoted = (
                len(value) >= 2
                and value[0] == value[-1]
                and value[0] in ("'", '"')
            )

            value = coerce_value(value, quoted=quoted)

            self._set_value(key, value)
            print(f"Updated {key}")

        except (KeyError, ValueError, IndexError) as e:
            print("Error:", e)

    def do_mkdir(self, arg):
        """Create an empty dict at a path: mkdir <path>"""
        target = arg.strip()
        if not target:
            print("Usage: mkdir <path>")
            return

        try:
            self._get_value(target)
            print(f"Already exists: {target}")
            return
        except KeyError:
            pass

        try:
            self._set_value(target, {})
            print(f"Created {target}")
        except KeyError:
            print("Key not found")

    def do_rm(self, arg):
        """Remove a key or list item at a path: rm <path>"""
        try:
            self._del_value(arg.strip())
            print(f"Removed {arg.strip()}")
        except KeyError:
            print("Key not found")

    def do_cd(self, arg):
        """Change the current node: cd <path>, cd .., cd, cd ../.."""
        target = arg.strip()
        try:
            parts, value = self._resolve_value(target)

            if not isinstance(value, (dict, list)):
                print("Not a branch")
                return

            self.cwd = parts
            self._update_prompt()
        except KeyError:
            print("Key not found")

    def do_pwd(self, arg):
        """Print the current path."""
        print(self._cwd_display())

    def do_save(self, arg):
        """Save changes to disk and clear the dirty flag: save [filename]"""
        target = arg.strip() or self.path
        save_file(target, self.raw)
        self.path = target
        self.dirty = False
        self._update_prompt()
        print("Saved")

    def do_reload(self, arg):
        """Reload the document from disk and discard unsaved changes."""
        self.raw = load_file(self.path)
        self.cwd = []
        self.dirty = False
        self._update_prompt()
        print("Reloaded")

    def do_exit(self, arg):
        """Exit the shell, warning first if there are unsaved changes."""
        if self.dirty and arg.strip().lower() not in ("y", "yes"):
            print("File changed, still exit? Use: exit yes")
            return False
        print("Bye")
        return True

    def do_quit(self, arg):
        """Exit the shell, warning first if there are unsaved changes."""
        return self.do_exit(arg)

    def do_EOF(self, arg):
        """Exit the shell on end-of-file, warning if there are unsaved changes."""
        print()
        return self.do_exit(arg)

    def complete_encode(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_ls(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_get(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_set(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_mkdir(self, text, line, begidx, endidx):
        return self._complete_keys(text, branches_only=True)

    def complete_rm(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_cd(self, text, line, begidx, endidx):
        return self._complete_keys(text, branches_only=True)

    def complete_show(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_tree(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def complete_yank(self, text, line, begidx, endidx):
        return self._complete_keys(text)

    def _tree_lines(self, value, prefix=""):
        """Yield Unix-style tree lines for a node's descendants."""
        entries = self._iter_children(value)
        for index, (name, child) in enumerate(entries):
            is_last = index == len(entries) - 1
            branch = "└── " if is_last else "├── "
            if isinstance(child, (dict, list)):
                yield f"{prefix}{branch}{name}/"
            else:
                yield f"{prefix}{branch}{name} [{child}]"

            if isinstance(child, (dict, list)):
                extension = "    " if is_last else "│   "
                yield from self._tree_lines(child, prefix + extension)

    def _match_child(self, parts, token, branches_only=True):
        """Resolve one path segment by exact match or a unique prefix match."""
        entries = self._iter_children(self._walk(parts) if parts else self.raw)
        if branches_only:
            entries = [
                (name, value)
                for name, value in entries
                if isinstance(value, (dict, list))
            ]

        exact = [name for name, _ in entries if name == token]
        if exact:
            return exact[0]

        prefix_matches = [name for name, _ in entries if name.startswith(token)]
        if len(prefix_matches) == 1:
            return prefix_matches[0]
        return None

    def _completion_context(self, text):
        """Parse partial input into parent node, base path, and active prefix.

        For example, ``ispic/d`` resolves to the ``ispic`` node, uses
        ``ispic/`` as the base path for completions, and ``d`` as the
        segment prefix to match.
        """
        absolute = text.startswith("/")
        trailing_slash = text.endswith("/")

        raw_parts = text.split("/")
        if absolute:
            raw_parts = raw_parts[1:]

        if trailing_slash:
            raw_parts.append("")

        if raw_parts:
            prefix = raw_parts[-1]
            path_parts = raw_parts[:-1]
        else:
            prefix = ""
            path_parts = []

        resolved_parts = [] if absolute else list(self.cwd)
        rendered_parts = []

        for part in path_parts:
            if part in ("", "."):
                continue
            if part == "..":
                if resolved_parts:
                    resolved_parts.pop()
                rendered_parts.append("..")
                continue

            match = self._match_child(resolved_parts, part, branches_only=True)
            if match is None:
                raise KeyError(part)

            resolved_parts.append(match)
            rendered_parts.append(match)

        if absolute:
            base = "/" + "/".join(rendered_parts) if rendered_parts else "/"
        else:
            base = "/".join(rendered_parts)

        if base and not base.endswith("/"):
            base += "/"

        return resolved_parts, base, prefix

    def _complete_keys(self, text, branches_only=False):
        """Return completion candidates for the current command path argument."""
        try:
            parent_parts, search_base, prefix = self._completion_context(text)
            entries = self._iter_children(self._walk(parent_parts) if parent_parts else self.raw)
        except KeyError:
            entries = []
            search_base = ""
            prefix = ""

        matches = []
        for name, value in entries:
            is_branch = isinstance(value, (dict, list))
            if branches_only and not is_branch:
                continue
            if name.startswith(prefix):
                candidate = f"{search_base}{name}"
                if is_branch:
                    candidate += "/"
                matches.append(candidate)

        if branches_only:
            specials = ["..", "/", "../.."]
            for item in specials:
                if item.startswith(text):
                    matches.append(item)

        return sorted(set(matches))
