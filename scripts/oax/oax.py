#!/usr/bin/env python3
"""oax — OpenAPI Explorer. Interactive shell for exploring OpenAPI 3.x specs."""

import cmd
import json
import os
import re
import shlex
from pprint import pformat
import sys
import textwrap
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None

try:
    import yaml
except ImportError:
    yaml = None

try:
    import jsonref
except ImportError:
    jsonref = None

try:
    from rich.console import Console
    from rich.table import Table
    from rich.syntax import Syntax
    from rich import print as rprint
    RICH = True
except ImportError:
    RICH = False

try:
    import pyperclip
except ImportError:
    pyperclip = None

__version__ = "26.6.3"


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

console = Console() if RICH else None


def _is_color():
    return OAX._instance and OAX._instance.opt_color and RICH


def out(text=""):
    if _is_color():
        console.print(text)
    else:
        print(text)


def err(text):
    if RICH and OAX._instance and OAX._instance.opt_color:
        console.print(f"[bold red]Error:[/bold red] {text}")
    else:
        print(f"Error: {text}", file=sys.stderr)


def header(text):
    if _is_color():
        console.print(f"\n[bold cyan]{text}[/bold cyan]")
    else:
        print(f"\n{text}")
        print("-" * len(text))


def kv(key, value, indent=0):
    pad = " " * indent
    if _is_color():
        console.print(f"{pad}[bold]{key}:[/bold] {value}")
    else:
        print(f"{pad}{key}: {value}")


def make_table(columns, rows, title=None):
    """Return a Rich Table when available, otherwise print plain text."""
    if RICH:
        t = Table(title=title, show_lines=False, header_style="bold magenta")
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(c) if c is not None else "" for c in row])
        console.print(t)
    else:
        if title:
            header(title)
        col_widths = [len(c) for c in columns]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell) if cell else ""))
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print(fmt.format(*columns))
        print("  ".join("-" * w for w in col_widths))
        for row in rows:
            print(fmt.format(*[str(c) if c is not None else "" for c in row]))


# ---------------------------------------------------------------------------
# Spec loading
# ---------------------------------------------------------------------------

def _load_raw(source: str) -> dict:
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        if requests is None:
            raise RuntimeError("requests library is required to fetch URLs. Install with: pip install requests")
        resp = requests.get(source, timeout=30)
        resp.raise_for_status()
        content = resp.text
        fmt = "yaml" if source.rstrip("?").endswith((".yaml", ".yml")) else "json"
        if "yaml" in resp.headers.get("content-type", ""):
            fmt = "yaml"
    else:
        if not os.path.exists(source):
            raise FileNotFoundError(f"File not found: {source}")
        with open(source, "r", encoding="utf-8") as f:
            content = f.read()
        fmt = "yaml" if source.endswith((".yaml", ".yml")) else "json"

    if fmt == "yaml":
        if yaml is None:
            raise RuntimeError("pyyaml is required for YAML specs. Install with: pip install pyyaml")
        raw = yaml.safe_load(content)
    else:
        raw = json.loads(content)

    return raw


def load_spec(source: str) -> dict:
    """Load and resolve an OpenAPI spec from a source."""
    _, resolved = _load_spec_data(source)
    return resolved


def _load_spec_data(source: str):
    """Return raw and resolved OpenAPI spec data for a source."""
    raw = _load_raw(source)
    version = str(raw.get("openapi", raw.get("swagger", "unknown")))
    if version.startswith("2."):
        raise ValueError(
            f"OpenAPI 2.x (Swagger) specs are not supported (detected version: {version}). "
            "Convert to OpenAPI 3.x first using a tool such as: "
            "https://converter.swagger.io/"
        )
    if not version.startswith("3."):
        raise ValueError(f"Unsupported spec version: {version}. Only OpenAPI 3.x is supported.")

    resolved = raw
    if jsonref is not None:
        base_uri = source if urlparse(source).scheme in ("http", "https") else f"file://{os.path.abspath(source)}"
        resolved = jsonref.replace_refs(
            raw,
            base_uri=base_uri,
            lazy_load=False,
            proxies=False,
        )
    return raw, resolved


# ---------------------------------------------------------------------------
# Spec query helpers
# ---------------------------------------------------------------------------

def _paths(spec):
    return spec.get("paths", {})


def _operations(spec):
    methods = ("get", "post", "put", "patch", "delete", "head", "options", "trace")
    for path, path_item in _paths(spec).items():
        for method in methods:
            op = path_item.get(method)
            if op is not None:
                yield path, method, op


def _find_op(spec, path, method):
    return _paths(spec).get(path, {}).get(method.lower())


def _raw_operation(source, path, method, raw_spec=None):
    """Return the raw operation from the original loaded source."""
    if raw_spec is not None:
        return _find_op(raw_spec, path, method)
    if not source:
        return None
    raw = _load_raw(source)
    return _find_op(raw, path, method)


def _raw_spec_or_resolved(source, resolved_spec, raw_spec=None):
    """Return raw spec data, falling back to resolved data if unavailable."""
    if raw_spec is not None:
        return raw_spec
    if source:
        try:
            return _load_raw(source)
        except Exception:
            return resolved_spec
    return resolved_spec


def _json_search_blob(value):
    """Return lower-case JSON text without failing on resolved reference cycles."""
    try:
        return json.dumps(value).lower()
    except (TypeError, ValueError):
        return ""


def _schemas(spec):
    return spec.get("components", {}).get("schemas", {})


def _security_schemes(spec):
    return spec.get("components", {}).get("securitySchemes", {})


def _unwrap_trivial_composition(schema):
    """Collapse single-item composition wrappers into their item schema."""
    current = schema
    while isinstance(current, dict):
        changed = False
        for key in ("allOf", "oneOf", "anyOf"):
            composed = current.get(key)
            if isinstance(composed, list) and len(composed) == 1:
                child = composed[0]
                if isinstance(child, dict):
                    current = child
                    changed = True
                    break
        if not changed:
            return current
    return current


def _unwrap_single_property_container(schema):
    """Collapse object wrappers that only contain one property."""
    current = _unwrap_trivial_composition(schema)
    while isinstance(current, dict):
        if current.get("type") != "object":
            return current
        properties = current.get("properties", {})
        if len(properties) != 1:
            return current
        child = next(iter(properties.values()))
        if not isinstance(child, dict):
            return current
        current = _unwrap_trivial_composition(child)
    return current


def _schema_details(schema):
    """Return compact detail strings for a schema node."""
    if not isinstance(schema, dict):
        return []
    details = []
    format_value = schema.get("format")
    if format_value:
        details.append(f"format={format_value}")
    if "example" in schema:
        details.append(f"example={schema['example']!r}")
    if "default" in schema:
        details.append(f"default={schema['default']!r}")
    enum_value = schema.get("enum")
    if enum_value:
        details.append(f"enum={enum_value!r}")
    if schema.get("nullable"):
        details.append("nullable")
    if schema.get("readOnly"):
        details.append("readOnly")
    if schema.get("writeOnly"):
        details.append("writeOnly")
    for key in ("minimum", "maximum", "minLength", "maxLength", "pattern", "multipleOf", "minItems", "maxItems"):
        if key in schema:
            details.append(f"{key}={schema[key]!r}")
    if schema.get("uniqueItems"):
        details.append("uniqueItems")
    return details


def _schema_shape_summary(schema):
    """Return a concise one-line summary of a schema shape."""
    schema = _unwrap_trivial_composition(schema)
    if not isinstance(schema, dict):
        return "object"
    ref = schema.get("$ref")
    if ref:
        return _ref_label(ref)
    for key in ("allOf", "oneOf", "anyOf"):
        composed = schema.get(key)
        if isinstance(composed, list) and composed:
            parts = [_schema_shape_summary(part) for part in composed[:3]]
            suffix = "..." if len(composed) > 3 else ""
            return f"{key}({', '.join(parts)}{suffix})"
    if schema.get("type") == "array":
        return f"array of {_schema_shape_summary(schema.get('items', {}))}"
    if schema.get("type") == "object" or "properties" in schema:
        props = list(schema.get("properties", {}).keys())
        if not props:
            return "object"
        preview = ", ".join(props[:4])
        if len(props) > 4:
            preview += ", ..."
        return f"object[{preview}]"
    return schema.get("type", "object")


def _schema_type_label(schema):
    """Return a display label for a schema type column."""
    schema = _unwrap_trivial_composition(schema)
    if not isinstance(schema, dict):
        return "object"
    ref = schema.get("$ref")
    if ref:
        return _ref_label(ref)
    return _schema_shape_summary(schema)


def _schema_example(schema, spec=None, visited=None):
    """Build a compact example value for a schema."""
    schema = _unwrap_trivial_composition(schema)
    if visited is None:
        visited = set()
    if not isinstance(schema, dict):
        return None
    schema_id = id(schema)
    if schema_id in visited:
        return None
    visited.add(schema_id)

    if "example" in schema:
        return schema["example"]
    if "default" in schema:
        return schema["default"]
    enum_value = schema.get("enum")
    if enum_value:
        return enum_value[0]

    ref = schema.get("$ref")
    if ref and spec is not None:
        target = _schemas(spec).get(_ref_label(ref))
        if target is not None:
            return _schema_example(target, spec, visited)

    schema_type = schema.get("type", "")
    if schema_type == "object" or "properties" in schema:
        result = {}
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        ordered = [name for name in properties if name in required]
        ordered.extend([name for name in properties if name not in required])
        for name in ordered:
            child = _schema_example(properties.get(name, {}), spec, visited)
            if child is not None:
                result[name] = child
        return result
    if schema_type == "array":
        item = _schema_example(schema.get("items", {}), spec, visited)
        return [item] if item is not None else []
    if schema_type == "boolean":
        return True
    if schema_type == "integer":
        return 0
    if schema_type == "number":
        return 0
    if schema_type == "string":
        if schema.get("format") == "date-time":
            return "2000-01-01 08:00:00"
        if schema.get("format") == "date":
            return "2000-01-01"
        if schema.get("format") == "uuid":
            return "00000000-0000-0000-0000-000000000000"
        return "string"
    return None


def _resolve_schema(schema, indent=0, visited=None, compact=False, detail_depth=0):
    """Return a flat list of schema rows."""
    if visited is None:
        visited = set()
    if not isinstance(schema, dict):
        return []
    schema = _unwrap_trivial_composition(schema)
    schema_id = id(schema)
    if schema_id in visited:
        return []
    visited.add(schema_id)
    lines = []
    schema_type = schema.get("type", "")
    description = schema.get("description", "")
    required_fields = set(schema.get("required", []))

    for key in ("allOf", "oneOf", "anyOf"):
        composed = schema.get(key)
        if isinstance(composed, list) and composed:
            lines.append((indent, key, "", "", description))
            for index, part in enumerate(composed, start=1):
                if not isinstance(part, dict):
                    lines.append((indent + 2, f"{key}[{index}]", str(part), "", ""))
                    continue
                ptype = _schema_type_label(part)
                pdesc = part.get("description", "")
                pdetails = ", ".join(_schema_details(part))
                lines.append((indent + 2, f"{key}[{index}]", ptype, pdetails, pdesc))
                if not compact or detail_depth > 0:
                    lines.extend(
                        _resolve_schema(
                            part,
                            indent + 4,
                            visited,
                            compact=compact,
                            detail_depth=max(detail_depth - 1, 0),
                        )
                    )

    if schema_type == "object" or "properties" in schema:
        for prop, prop_schema in schema.get("properties", {}).items():
            req = "*" if prop in required_fields else ""
            ptype = _schema_type_label(prop_schema)
            pdesc = prop_schema.get("description", "")
            pdetails = ", ".join(_schema_details(prop_schema))
            lines.append((indent, f"{prop}{req}", ptype, pdetails, pdesc))
            if not compact or detail_depth > 0:
                if (
                    prop_schema.get("type") == "object"
                    or "properties" in prop_schema
                    or any(k in prop_schema for k in ("allOf", "oneOf", "anyOf"))
                ):
                    lines.extend(
                        _resolve_schema(
                            prop_schema,
                            indent + 2,
                            visited,
                            compact=compact,
                            detail_depth=max(detail_depth - 1, 0),
                        )
                    )
                elif prop_schema.get("type") == "array":
                    items = prop_schema.get("items", {})
                    lines.extend(
                        _resolve_schema(
                            items,
                            indent + 2,
                            visited,
                            compact=compact,
                            detail_depth=max(detail_depth - 1, 0),
                        )
                    )
    elif schema_type == "array":
        items = _unwrap_trivial_composition(schema.get("items", {}))
        lines.append((indent, "items", _schema_type_label(items), ", ".join(_schema_details(items)), items.get("description", "")))
        if not compact or detail_depth > 0:
            lines.extend(
                _resolve_schema(
                    items,
                    indent + 2,
                    visited,
                    compact=compact,
                    detail_depth=max(detail_depth - 1, 0),
                )
            )
    return lines


def _request_body_schemas(op):
    """Return request-body content types and schemas for an operation."""
    rb = op.get("requestBody")
    if not rb:
        return None, []
    schemas = []
    for content_type, media in rb.get("content", {}).items():
        schemas.append((content_type, media.get("schema", {})))
    return rb, schemas


def _collect_schema_refs(schema, refs=None):
    """Collect referenced schema names from a raw schema tree."""
    if refs is None:
        refs = []
    if not isinstance(schema, dict):
        return refs
    ref = schema.get("$ref")
    if ref:
        refs.append(ref)
    for key in ("properties", "items", "allOf", "oneOf", "anyOf"):
        value = schema.get(key)
        if isinstance(value, dict):
            if key == "properties":
                for child in value.values():
                    _collect_schema_refs(child, refs)
            else:
                _collect_schema_refs(value, refs)
        elif isinstance(value, list):
            for child in value:
                _collect_schema_refs(child, refs)
    return refs


def _ref_label(ref):
    """Return the short label for a schema reference."""
    if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
        return ref.rsplit("/", 1)[-1]
    return str(ref)


def _render_schema(schema, refs=None, compact=False):
    """Render a schema as a property table."""
    inst = OAX._instance
    show_details = bool(inst and inst.opt_details)
    display_schema = _unwrap_single_property_container(schema) if compact else schema
    out(f"  Shape: {_schema_shape_summary(display_schema)}")
    if refs:
        labels = ", ".join(_ref_label(ref) for ref in refs)
        out(f"  References: {labels}")
    detail_depth = 1 if compact and show_details else 0
    lines = _resolve_schema(display_schema, compact=compact, detail_depth=detail_depth)
    if lines:
        if show_details:
            rows = [(" " * ind + name, typ, details, desc) for ind, name, typ, details, desc in lines]
            make_table(["Property *=required", "Type", "Details", "Description"], rows)
        else:
            rows = [(" " * ind + name, typ, desc) for ind, name, typ, details, desc in lines]
            make_table(["Property *=required", "Type", "Description"], rows)
    else:
        out(f"  Type: {display_schema.get('type', 'object')}")


def _model_name(name: str) -> str:
    parts = re.split(r"[_\-]+", name)
    converted = []
    for part in parts:
        if not part:
            continue
        if any(ch.isupper() for ch in part[1:]):
            converted.append(part)
        else:
            converted.append(part[:1].upper() + part[1:].lower())
    return "".join(converted)


def _generate_pydantic_model(name: str, schema: dict, name_map: dict[int, str], spec: dict) -> list[str]:
    """Return lines of Python source for a Pydantic model."""
    lines = [f"class {_model_name(name)}(BaseModel):"]
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    if not properties:
        lines.append("    pass")
        return lines
    for prop, prop_schema in properties.items():
        py_type = _schema_python_type(prop_schema, name_map, spec)
        safe_name = prop.replace("-", "_")
        if prop in required:
            lines.append(f"    {safe_name}: {py_type}")
        else:
            default = prop_schema.get("default")
            if default is None:
                lines.append(f"    {safe_name}: Optional[{py_type}] = None")
            else:
                lines.append(f"    {safe_name}: {py_type} = {repr(default)}")
    return lines


def _collect_models(schema: dict, name: str, spec: dict) -> dict[str, dict]:
    """Walk a schema and collect named object schemas worth generating as models."""
    models, _ = _collect_model_artifacts(schema, name, spec)
    return models


def _collect_model_artifacts(schema: dict, name: str, spec: dict, models=None, name_map=None):
    """Collect model definitions and a schema-id to model-name mapping."""
    if models is None:
        models = {}
    if name_map is None:
        name_map = {}
    if not isinstance(schema, dict):
        return models, name_map
    schema = _unwrap_trivial_composition(schema)
    if not isinstance(schema, dict):
        return models, name_map
    ref = schema.get("$ref")
    if ref and spec is not None:
        target = _schemas(spec).get(_ref_label(ref))
        if target is not None:
            return _collect_model_artifacts(target, name, spec, models, name_map)
    schema_type = schema.get("type", "")
    if schema_type == "object" or "properties" in schema:
        models[name] = schema
        name_map[id(schema)] = _model_name(name)
        for prop, prop_schema in schema.get("properties", {}).items():
            _collect_model_artifacts(prop_schema, f"{name}_{prop}", spec, models, name_map)
    elif schema_type == "array":
        items = schema.get("items", {})
        _collect_model_artifacts(items, name, spec, models, name_map)
    return models, name_map


def _schema_python_type(schema: dict, name_map=None, spec: dict | None = None) -> str:
    """Return the Python type annotation for an OpenAPI schema node."""
    if not isinstance(schema, dict):
        return "Any"
    schema = _unwrap_trivial_composition(schema)
    if not isinstance(schema, dict):
        return "Any"
    ref = schema.get("$ref")
    if ref:
        if spec is not None and name_map is not None:
            target = _schemas(spec).get(_ref_label(ref))
            if target is not None and id(target) in name_map:
                return name_map[id(target)]
        return _model_name(_ref_label(ref))
    schema_type = schema.get("type", "")
    if schema_type == "array":
        return f"List[{_schema_python_type(schema.get('items', {}), name_map, spec)}]"
    if schema_type == "object" or "properties" in schema:
        if name_map is not None and id(schema) in name_map:
            return name_map[id(schema)]
        return "Dict[str, Any]"
    return {
        "string": "str",
        "integer": "int",
        "number": "float",
        "boolean": "bool",
    }.get(schema_type, "Any")


def _schema_model_source(name: str, schema: dict, spec: dict, include_example=False) -> str:
    """Generate a Pydantic model module for a component schema."""
    models, name_map = _collect_model_artifacts(schema, name, spec)
    if not models:
        models = {name: schema}
        name_map = {id(schema): _model_name(name)}

    lines = []
    lines.append("from __future__ import annotations")
    lines.append("from pprint import pformat")
    lines.append("from typing import Any, Dict, List, Optional")
    lines.append("from pydantic import BaseModel")
    lines.append("")

    for mname, mschema in models.items():
        lines.extend(_generate_pydantic_model(mname, mschema, name_map, spec))
        lines.append("")

    if include_example:
        model_class = _model_name(name)
        example = _schema_example(schema, spec)
        if example is not None:
            lines.append(f"{model_class}_EXAMPLE = {model_class}.model_validate(")
            for line in pformat(example, sort_dicts=False, width=88).splitlines():
                lines.append(f"    {line}")
            lines.append(")")

    return "\n".join(lines).rstrip()


# ---------------------------------------------------------------------------
# Main shell
# ---------------------------------------------------------------------------

class OAX(cmd.Cmd):
    intro = "oax — OpenAPI Explorer. Type 'help' for commands, 'quit' to exit."
    prompt = "oax> "
    _instance = None

    def __init__(self):
        super().__init__()
        OAX._instance = self
        self.spec = None
        self.raw_spec = None
        self.source = None
        self.history_list = []
        self.opt_verbose = False
        self.opt_details = False
        self.opt_color = True
        try:
            import readline
            # Remove '/' and '{' from delimiters so path completion works on full paths like /foo/{id}
            delims = readline.get_completer_delims()
            readline.set_completer_delims(delims.replace("/", "").replace("{", "").replace("}", ""))
        except ImportError:
            pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_spec(self):
        if self.spec is None:
            err("No spec loaded. Use: load <url|file>")
            return False
        return True

    def _record_history(self, path, method):
        entry = (path, method.upper())
        if not self.history_list or self.history_list[-1] != entry:
            self.history_list.append(entry)
        if len(self.history_list) > 50:
            self.history_list.pop(0)

    def _paths_list(self):
        if self.spec is None:
            return []
        return list(_paths(self.spec).keys())

    def _schemas_list(self):
        if self.spec is None:
            return []
        return list(_schemas(self.spec).keys())

    def _tags_list(self):
        if self.spec is None:
            return []
        tags = {t["name"] for t in self.spec.get("tags", []) if isinstance(t, dict) and t.get("name")}
        for _, _, op in _operations(self.spec):
            for t in op.get("tags", []):
                tags.add(t)
        return sorted(tags)

    def _strip_wrapping_quotes(self, value):
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            return value[1:-1]
        return value

    def _split_path_method(self, arg, require_method=True):
        parts = arg.split()
        if require_method:
            if len(parts) < 2:
                err("Usage: <path> <method>")
                return None, None
            return parts[0], parts[1].lower()
        return parts[0] if parts else None, None

    # ------------------------------------------------------------------
    # Tab completion helpers
    # ------------------------------------------------------------------

    def _complete_path(self, text):
        return [p for p in self._paths_list() if p.startswith(text)]

    def _complete_path_method(self, text, line, begidx, endidx):
        words = line[:begidx].split()
        if len(words) == 1:
            return self._complete_path(text)
        if len(words) == 2 and self.spec:
            path = words[1]
            methods = ("get", "post", "put", "patch", "delete", "head", "options", "trace")
            path_item = _paths(self.spec).get(path, {})
            available = [m for m in methods if m in path_item]
            return [m for m in available if m.startswith(text)]
        return []

    def complete_path(self, text, line, begidx, endidx):
        return self._complete_path(text)

    def complete_op(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_params(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_body(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_responses(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_response(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_auth(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_export(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def complete_schema(self, text, line, begidx, endidx):
        return [s for s in self._schemas_list() if s.startswith(text)]

    def complete_refs(self, text, line, begidx, endidx):
        return [s for s in self._schemas_list() if s.startswith(text)]

    def complete_example(self, text, line, begidx, endidx):
        return [s for s in self._schemas_list() if s.startswith(text)]

    def complete_model(self, text, line, begidx, endidx):
        return [s for s in self._schemas_list() if s.startswith(text)]

    def complete_tag(self, text, line, begidx, endidx):
        return [t for t in self._tags_list() if t.startswith(text)]

    def complete_load(self, text, line, begidx, endidx):
        if not text:
            return os.listdir(".")
        dirname = os.path.dirname(text)
        base = os.path.basename(text)
        try:
            entries = os.listdir(dirname or ".")
        except OSError:
            return []
        return [
            os.path.join(dirname, e) if dirname else e
            for e in entries
            if e.startswith(base)
        ]

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    def do_load(self, arg):
        """load <url|file>  Load an OpenAPI spec from a URL or local file."""
        source = arg.strip()
        if not source:
            err("Usage: load <url|file>")
            return
        out(f"Loading {source} ...")
        try:
            raw, resolved = _load_spec_data(source)
            self.raw_spec = raw
            self.spec = resolved
            self.source = source
            title = self.spec.get("info", {}).get("title", "Untitled")
            version = self.spec.get("info", {}).get("version", "?")
            out(f"Loaded: [bold]{title}[/bold] v{version}" if _is_color() else f"Loaded: {title} v{version}")
        except Exception as e:
            err(str(e))

    def do_info(self, arg):
        """info  Show top-level metadata for the loaded spec."""
        if not self._require_spec():
            return
        info = self.spec.get("info", {})
        header("Spec Info")
        kv("Title", info.get("title", ""))
        kv("Version", info.get("version", ""))
        kv("Description", info.get("description", ""))
        contact = info.get("contact", {})
        if contact:
            kv("Contact", f"{contact.get('name','')} <{contact.get('email','')}>  {contact.get('url','')}")
        lic = info.get("license", {})
        if lic:
            kv("License", f"{lic.get('name','')}  {lic.get('url','')}")
        servers = self.spec.get("servers", [])
        if servers:
            header("Servers")
            for s in servers:
                kv("URL", s.get("url", ""))
                if s.get("description"):
                    kv("  Desc", s["description"])

    def do_stats(self, arg):
        """stats  Show operation, schema, and tag counts."""
        if not self._require_spec():
            return
        from collections import Counter
        method_counts = Counter()
        for _, method, _ in _operations(self.spec):
            method_counts[method.upper()] += 1
        header("Stats")
        kv("Paths", len(_paths(self.spec)))
        kv("Operations", sum(method_counts.values()))
        for method, count in sorted(method_counts.items()):
            kv(f"  {method}", count)
        kv("Schemas", len(_schemas(self.spec)))
        kv("Tags", len(self._tags_list()))
        kv("Security schemes", len(_security_schemes(self.spec)))

    def do_paths(self, arg):
        """paths [filter]  List all paths, with optional substring filter."""
        if not self._require_spec():
            return
        filt = arg.strip()
        rows = []
        for path, path_item in _paths(self.spec).items():
            if filt and filt not in path:
                continue
            methods = [m.upper() for m in ("get","post","put","patch","delete","head","options","trace") if m in path_item]
            rows.append((path, ", ".join(methods)))
        if not rows:
            out("No paths found.")
            return
        rows.sort(key=lambda r: r[0])
        make_table(["Path", "Methods"], rows, title="Paths")

    def do_path(self, arg):
        """path <path>  Show all operations defined on a path."""
        if not self._require_spec():
            return
        path = arg.strip()
        if not path:
            err("Usage: path <path>")
            return
        path_item = _paths(self.spec).get(path)
        if path_item is None:
            err(f"Path not found: {path}")
            return
        header(path)
        rows = []
        for method in ("get","post","put","patch","delete","head","options","trace"):
            op = path_item.get(method)
            if op:
                rows.append((method.upper(), op.get("operationId",""), op.get("summary","")))
        make_table(["Method", "Operation ID", "Summary"], rows)

    def do_op(self, arg):
        """op <path> <method>  Show full operation detail."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        header(f"{method.upper()} {path}")
        kv("Operation ID", op.get("operationId", ""))
        kv("Summary", op.get("summary", ""))
        if self.opt_verbose:
            kv("Description", op.get("description", ""))
        kv("Tags", ", ".join(op.get("tags", [])))
        params = op.get("parameters", [])
        raw_op = _raw_operation(self.source, path, method, self.raw_spec)
        if params:
            out("\nParameters:")
            rows = [(p.get("name",""), p.get("in",""), p.get("schema",{}).get("type",""), "yes" if p.get("required") else "no", p.get("description","")) for p in params]
            make_table(["Name", "In", "Type", "Required", "Description"], rows)
        rb, schemas = _request_body_schemas(op)
        if rb:
            out(f"\nRequest Body (required={rb.get('required', False)}):")
            raw_content = {}
            if raw_op:
                raw_content = raw_op.get("requestBody", {}).get("content", {})
            for ct, schema in schemas:
                refs = _collect_schema_refs(
                    raw_content.get(ct, {}).get("schema", {})
                )
                out(f"  Content-Type: {ct}")
                if refs:
                    out(f"  Schema: {', '.join(_ref_label(ref) for ref in refs)}")
                else:
                    out(f"  Schema: {schema.get('type', 'object')}")
        out("\nResponses:")
        rows = [(code, resp.get("description","")) for code, resp in op.get("responses",{}).items()]
        make_table(["Code", "Description"], rows)

    def do_tags(self, arg):
        """tags  List all tags in the spec."""
        if not self._require_spec():
            return
        defined = {t["name"]: t.get("description","") for t in self.spec.get("tags",[])}
        all_tags = self._tags_list()
        if not all_tags:
            out("No tags found.")
            return
        rows = [(t, defined.get(t,"")) for t in all_tags]
        make_table(["Tag", "Description"], rows, title="Tags")

    def do_tag(self, arg):
        """tag <name>  List all operations with the given tag."""
        if not self._require_spec():
            return
        name = self._strip_wrapping_quotes(arg.strip())
        if not name:
            err("Usage: tag <name>")
            return
        rows = []
        for path, method, op in _operations(self.spec):
            if name in op.get("tags", []):
                rows.append((method.upper(), path, op.get("summary","")))
        if not rows:
            out(f"No operations found with tag: {name}")
            return
        make_table(["Method", "Path", "Summary"], rows, title=f"Tag: {name}")

    def do_params(self, arg):
        """params <path> <method>  List all parameters for an operation."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        params = op.get("parameters", [])
        rb, schemas = _request_body_schemas(op)
        raw_op = _raw_operation(self.source, path, method, self.raw_spec)
        if not params and not rb:
            out("No parameters defined.")
            return
        if params:
            if self.opt_details:
                rows = [
                    (
                        p.get("name", ""),
                        p.get("in", ""),
                        p.get("schema", {}).get("type", ""),
                        ", ".join(_schema_details(p.get("schema", {}))),
                        "yes" if p.get("required") else "no",
                        p.get("description", "") if self.opt_verbose else p.get("description", "")[:60],
                    )
                    for p in params
                ]
                make_table(
                    ["Name", "In", "Type", "Details", "Required", "Description"],
                    rows,
                    title=f"Params: {method.upper()} {path}",
                )
            else:
                rows = [
                    (
                        p.get("name", ""),
                        p.get("in", ""),
                        p.get("schema", {}).get("type", ""),
                        "yes" if p.get("required") else "no",
                        p.get("description", "") if self.opt_verbose else p.get("description", "")[:60],
                    )
                    for p in params
                ]
                make_table(
                    ["Name", "In", "Type", "Required", "Description"],
                    rows,
                    title=f"Params: {method.upper()} {path}",
                )
        if rb:
            out(f"\nRequest Body (required={rb.get('required', False)}):")
            raw_content = {}
            if raw_op:
                raw_content = raw_op.get("requestBody", {}).get("content", {})
            for ct, schema in schemas:
                refs = _collect_schema_refs(
                    raw_content.get(ct, {}).get("schema", {})
                )
                out(f"  Content-Type: {ct}")
                _render_schema(schema, refs, compact=True)

    def do_body(self, arg):
        """body <path> <method>  Show the request body schema."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        rb, schemas = _request_body_schemas(op)
        if not rb:
            out("No request body defined.")
            return
        header(f"Request Body: {method.upper()} {path}")
        kv("Required", rb.get("required", False))
        raw_op = _raw_operation(self.source, path, method, self.raw_spec)
        raw_content = {}
        if raw_op:
            raw_content = raw_op.get("requestBody", {}).get("content", {})
        for ct, schema in schemas:
            refs = _collect_schema_refs(raw_content.get(ct, {}).get("schema", {}))
            out(f"\nContent-Type: {ct}")
            _render_schema(schema, refs, compact=True)

    def do_responses(self, arg):
        """responses <path> <method>  List response codes for an operation."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        resps = op.get("responses", {})
        if not resps:
            out("No responses defined.")
            return
        rows = [(code, resp.get("description","")) for code, resp in resps.items()]
        make_table(["Code", "Description"], rows, title=f"Responses: {method.upper()} {path}")

    def do_response(self, arg):
        """response <path> <method> <code>  Show schema for a specific response code."""
        if not self._require_spec():
            return
        parts = arg.split()
        if len(parts) < 3:
            err("Usage: response <path> <method> <code>")
            return
        path, method, code = parts[0], parts[1].lower(), parts[2]
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        resp = op.get("responses", {}).get(code)
        if resp is None:
            err(f"Response code {code} not found for {method.upper()} {path}")
            return
        header(f"Response {code}: {method.upper()} {path}")
        kv("Description", resp.get("description",""))
        raw_op = _raw_operation(self.source, path, method, self.raw_spec)
        raw_content = {}
        if raw_op:
            raw_content = raw_op.get("responses", {}).get(code, {}).get("content", {})
        for ct, media in resp.get("content", {}).items():
            out(f"\nContent-Type: {ct}")
            schema = media.get("schema", {})
            refs = _collect_schema_refs(raw_content.get(ct, {}).get("schema", {}))
            _render_schema(schema, refs, compact=True)

    def do_example(self, arg):
        """example <schema>  Show a synthesized JSON example for a component schema."""
        if not self._require_spec():
            return
        name = arg.strip()
        if not name:
            err("Usage: example <schema>")
            return
        schema = _schemas(self.spec).get(name)
        if schema is None:
            err(f"Schema not found: {name}")
            return
        example = _schema_example(schema, self.spec)
        if example is None:
            out("No example could be synthesized.")
            return
        header(f"Example: {name}")
        rendered = json.dumps(example, indent=2, ensure_ascii=False)
        if RICH:
            console.print(Syntax(rendered, "json", theme="monokai"))
        else:
            print(rendered)

    def do_schemas(self, arg):
        """schemas  List all component schemas."""
        if not self._require_spec():
            return
        sc = _schemas(self.spec)
        if not sc:
            out("No schemas defined.")
            return
        rows = [(name, s.get("type","object"), s.get("description","")) for name, s in sc.items()]
        make_table(["Name", "Type", "Description"], rows, title="Schemas")

    def do_schema(self, arg):
        """schema <name>  Show a schema definition."""
        if not self._require_spec():
            return
        name = arg.strip()
        if not name:
            err("Usage: schema <name>")
            return
        schema = _schemas(self.spec).get(name)
        if schema is None:
            err(f"Schema not found: {name}")
            return
        header(f"Schema: {name}")
        if self.opt_verbose and schema.get("description"):
            kv("Description", schema["description"])
        _render_schema(schema, compact=True)

    def do_refs(self, arg):
        """refs <name>  Find all operations and schemas that reference the named schema."""
        if not self._require_spec():
            return
        name = arg.strip()
        if not name:
            err("Usage: refs <name>")
            return
        ref_token = f'"$ref": "' # after jsonref resolution, $refs are gone — search original
        target = f"#/components/schemas/{name}"
        raw = self.raw_spec if self.raw_spec is not None else _load_raw(self.source)
        raw_str = json.dumps(raw)
        if target not in raw_str:
            out(f"No references found to: {name}")
            return
        # Walk operations and schemas to find references
        rows = []
        for path, method, op in _operations(self.spec):
            op_str = json.dumps(raw.get("paths",{}).get(path,{}).get(method,{}))
            if target in op_str:
                rows.append(("operation", f"{method.upper()} {path}"))
        for sname, schema in _schemas(raw).items():
            if sname == name:
                continue
            if target in json.dumps(schema):
                rows.append(("schema", sname))
        if rows:
            make_table(["Type", "Reference"], rows, title=f"References to: {name}")
        else:
            out(f"No references found to: {name}")

    def do_security(self, arg):
        """security  List all security schemes."""
        if not self._require_spec():
            return
        schemes = _security_schemes(self.spec)
        if not schemes:
            out("No security schemes defined.")
            return
        rows = []
        for name, scheme in schemes.items():
            stype = scheme.get("type","")
            detail = scheme.get("scheme","") or scheme.get("flows","") or scheme.get("openIdConnectUrl","") or ""
            if isinstance(detail, dict):
                detail = ", ".join(detail.keys())
            rows.append((name, stype, str(detail)))
        make_table(["Name", "Type", "Detail"], rows, title="Security Schemes")

    def do_auth(self, arg):
        """auth <path> <method>  Show security requirements for an operation."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        sec = op.get("security")
        source_label = "operation"
        if sec is None:
            sec = self.spec.get("security")
            source_label = "global (inherited)"
        header(f"Security: {method.upper()} {path}")
        if not sec:
            out("No security required (public endpoint).")
            return
        out(f"Source: {source_label}")
        for req in sec:
            if not req:
                out("  (empty — no auth required)")
            for scheme, scopes in req.items():
                kv(f"  {scheme}", ", ".join(scopes) if scopes else "(no scopes)")

    def do_search(self, arg):
        """search <term>  Full-text search across paths, operation data, schemas, and schema metadata."""
        if not self._require_spec():
            return
        term = arg.strip().lower()
        if not term:
            err("Usage: search <term>")
            return
        search_spec = _raw_spec_or_resolved(
            self.source,
            self.spec,
            self.raw_spec,
        )
        rows = []
        for path, method, op in _operations(search_spec):
            op_blob = _json_search_blob(op)
            summary = op.get("summary", "")
            description = op.get("description", "")
            operation_id = op.get("operationId", "")
            if (
                term in path.lower()
                or term in summary.lower()
                or term in description.lower()
                or term in operation_id.lower()
                or term in op_blob
            ):
                rows.append(("operation", f"{method.upper()} {path}", summary))
        for name, schema in _schemas(search_spec).items():
            schema_blob = _json_search_blob(schema)
            if term in name.lower() or term in schema_blob:
                rows.append(("schema", name, ""))
        for name, scheme in _security_schemes(search_spec).items():
            scheme_blob = _json_search_blob(scheme)
            if term in name.lower() or term in scheme_blob:
                rows.append(("security", name, scheme.get("type","")))
        if rows:
            title = f'Search: "{arg.strip()}"'
            make_table(["Category", "Match", "Summary"], rows, title=title)
        else:
            out(f"No results for: {arg.strip()}")

    def do_export(self, arg):
        """export <path> <method>  Print a curl command example for the operation."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        op = _find_op(self.spec, path, method)
        if op is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        servers = self.spec.get("servers", [{}])
        base_url = servers[0].get("url","https://api.example.com").rstrip("/")

        url_path = path
        query_parts = []
        headers = []

        for p in op.get("parameters", []):
            pname = p.get("name","param")
            placeholder = f"<{pname}>"
            loc = p.get("in","")
            if loc == "path":
                url_path = url_path.replace(f"{{{pname}}}", placeholder)
            elif loc == "query":
                query_parts.append(f"{pname}={placeholder}")
            elif loc == "header":
                headers.append(f'-H "{pname}: {placeholder}"')

        url = base_url + url_path
        if query_parts:
            url += "?" + "&".join(query_parts)

        parts = [f"curl -X {method.upper()}"]
        parts.append(f'"{url}"')
        parts.extend(headers)

        rb = op.get("requestBody")
        if rb:
            for ct in rb.get("content", {}):
                parts.append(f'-H "Content-Type: {ct}"')
                parts.append("-d '{}'")
                break

        cmd_str = " \\\n  ".join(parts)
        header(f"curl example: {method.upper()} {path}")
        if RICH and _is_color():
            console.print(Syntax(cmd_str, "bash", theme="monokai"))
        else:
            print(cmd_str)

    def do_model(self, arg):
        """model <schema> [--example] [--example-only] [--json] [--copy] [--file <path>]  Export a Pydantic model for a schema."""
        if not self._require_spec():
            return
        try:
            parts = shlex.split(arg)
        except ValueError as exc:
            err(str(exc))
            return
        if not parts:
            err("Usage: model <schema> [--example] [--example-only] [--json] [--copy] [--file <path>]")
            return
        name = parts.pop(0)
        include_example = False
        example_only = False
        json_output = False
        copy_to_clipboard = False
        file_path = None
        idx = 0
        while idx < len(parts):
            token = parts[idx]
            if token in ("--example", "-e"):
                include_example = True
            elif token == "--example-only":
                include_example = True
                example_only = True
            elif token == "--json":
                json_output = True
            elif token == "--copy":
                copy_to_clipboard = True
            elif token == "--file":
                idx += 1
                if idx >= len(parts):
                    err("model: --file requires a path")
                    return
                file_path = parts[idx]
            else:
                err(f"Unknown option: {token}")
                err("Usage: model <schema> [--example] [--example-only] [--json] [--copy] [--file <path>]")
                return
            idx += 1

        schema = _schemas(self.spec).get(name)
        if schema is None:
            err(f"Schema not found: {name}")
            return

        if example_only:
            example = _schema_example(schema, self.spec)
            if example is None:
                out("No example could be synthesized.")
                return
            if json_output:
                rendered = json.dumps(example, indent=2, ensure_ascii=False)
            else:
                rendered = pformat(example, sort_dicts=False, width=88)
        else:
            if json_output:
                err("--json is only supported with --example-only")
                return
            rendered = _schema_model_source(name, schema, self.spec, include_example=include_example)
        if file_path:
            with open(file_path, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
            if example_only:
                out(f"wrote example to: {file_path}")
            else:
                out(f"wrote model to: {file_path}")
            return
        if copy_to_clipboard:
            if pyperclip is None:
                err("pyperclip is not installed; clipboard export is unavailable")
                return
            pyperclip.copy(rendered + "\n")
            out("example copied to clipboard" if example_only else "model copied to clipboard")
            return

        header(f"{'example' if example_only else 'model'}: {name}")
        if RICH and _is_color():
            console.print(Syntax(rendered, "python", theme="monokai", line_numbers=True))
        else:
            print(rendered)

    def do_history(self, arg):
        """history  Show recently visited operations."""
        if not self.history_list:
            out("No history yet.")
            return
        rows = [(i + 1, method, path) for i, (path, method) in enumerate(self.history_list)]
        make_table(["#", "Method", "Path"], rows, title="History")

    def do_set(self, arg):
        """set <option> <value>  Toggle display options (verbose, color, details)."""
        parts = arg.split()
        if len(parts) != 2:
            err("Usage: set <option> <value>")
            out("Options: verbose on|off  /  color on|off  /  details on|off")
            return
        option, value = parts[0].lower(), parts[1].lower()
        if option == "verbose":
            if value not in ("on","off"):
                err("verbose: expected 'on' or 'off'")
                return
            self.opt_verbose = value == "on"
            out(f"verbose = {value}")
        elif option == "color":
            if value not in ("on","off"):
                err("color: expected 'on' or 'off'")
                return
            if value == "on" and not RICH:
                err("rich library not installed; color unavailable. Install with: pip install rich")
                return
            self.opt_color = value == "on"
            out(f"color = {value}")
        elif option == "details":
            if value not in ("on","off"):
                err("details: expected 'on' or 'off'")
                return
            self.opt_details = value == "on"
            out(f"details = {value}")
        else:
            err(f"Unknown option: {option}")
            out("Options: verbose on|off  /  color on|off  /  details on|off")

    def do_quit(self, arg):
        """quit  Exit oax."""
        out("Bye.")
        return True

    def do_exit(self, arg):
        """exit  Exit oax."""
        return self.do_quit(arg)

    def do_EOF(self, arg):
        out("")
        return self.do_quit(arg)

    def emptyline(self):
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(
        prog="oax",
        description="oax — Interactive OpenAPI 3.x Explorer"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument("source", nargs="?", help="URL or file path to an OpenAPI spec")
    args = parser.parse_args()

    shell = OAX()

    if args.source:
        shell.do_load(args.source)

    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        out("\nInterrupted.")


if __name__ == "__main__":
    main()
