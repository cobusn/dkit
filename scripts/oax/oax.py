#!/usr/bin/env python3
"""oax — OpenAPI Explorer. Interactive shell for exploring OpenAPI 3.x specs."""

__version__ = "v26.06.1"

import cmd
import json
import os
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
    """Return a Rich Table or print a plain text table depending on settings."""
    inst = OAX._instance
    use_table = inst and inst.opt_format == "table"
    if RICH and _is_color() and use_table:
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

    if jsonref is not None:
        base_uri = source if urlparse(source).scheme in ("http", "https") else f"file://{os.path.abspath(source)}"
        resolved = jsonref.replace_refs(
            raw,
            base_uri=base_uri,
            lazy_load=False,
            proxies=False,
        )
        return resolved
    return raw


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


def _schemas(spec):
    return spec.get("components", {}).get("schemas", {})


def _security_schemes(spec):
    return spec.get("components", {}).get("securitySchemes", {})


def _resolve_schema(schema, indent=0, visited=None):
    """Return a flat list of (indent, label, type, description) tuples."""
    if visited is None:
        visited = set()
    if not isinstance(schema, dict):
        return []
    lines = []
    schema_type = schema.get("type", "")
    description = schema.get("description", "")
    required_fields = set(schema.get("required", []))

    if schema_type == "object" or "properties" in schema:
        for prop, prop_schema in schema.get("properties", {}).items():
            req = "*" if prop in required_fields else ""
            ptype = prop_schema.get("type", prop_schema.get("$ref", "object"))
            pdesc = prop_schema.get("description", "")
            lines.append((indent, f"{prop}{req}", ptype, pdesc))
            if prop_schema.get("type") == "object" or "properties" in prop_schema:
                lines.extend(_resolve_schema(prop_schema, indent + 2, visited))
            elif prop_schema.get("type") == "array":
                items = prop_schema.get("items", {})
                lines.extend(_resolve_schema(items, indent + 2, visited))
    elif schema_type == "array":
        items = schema.get("items", {})
        lines.append((indent, "items", items.get("type", "object"), items.get("description", "")))
        lines.extend(_resolve_schema(items, indent + 2, visited))
    return lines


# ---------------------------------------------------------------------------
# Code generation helpers
# ---------------------------------------------------------------------------

_OA_TYPE_MAP = {
    "string": "str",
    "integer": "int",
    "number": "float",
    "boolean": "bool",
    "object": "Dict[str, Any]",
    "array": "List[Any]",
}


def _py_type(schema: dict) -> str:
    if not isinstance(schema, dict):
        return "Any"
    oa_type = schema.get("type", "")
    if oa_type == "array":
        items = schema.get("items", {})
        return f"List[{_py_type(items)}]"
    if oa_type == "object" or "properties" in schema:
        return "Dict[str, Any]"
    return _OA_TYPE_MAP.get(oa_type, "Any")


def _model_name(name: str) -> str:
    return "".join(part.capitalize() for part in name.replace("-", "_").split("_"))


def _generate_pydantic_model(name: str, schema: dict) -> list[str]:
    """Return lines of Python source for a Pydantic model."""
    lines = [f"class {_model_name(name)}(BaseModel):"]
    properties = schema.get("properties", {})
    required = set(schema.get("required", []))
    if not properties:
        lines.append("    pass")
        return lines
    for prop, prop_schema in properties.items():
        py_type = _py_type(prop_schema)
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
    models = {}
    if not isinstance(schema, dict):
        return models
    if schema.get("type") == "object" or "properties" in schema:
        models[name] = schema
        for prop, prop_schema in schema.get("properties", {}).items():
            models.update(_collect_models(prop_schema, _model_name(f"{name}_{prop}"), spec))
    elif schema.get("type") == "array":
        items = schema.get("items", {})
        models.update(_collect_models(items, name, spec))
    return models


def generate_codegen(spec: dict, path: str, method: str) -> str:
    op = _find_op(spec, path, method)
    if op is None:
        return ""

    servers = spec.get("servers", [{}])
    base_url = servers[0].get("url", "https://api.example.com").rstrip("/")

    params = op.get("parameters", [])
    path_params = [p for p in params if p.get("in") == "path"]
    query_params = [p for p in params if p.get("in") == "query"]
    header_params = [p for p in params if p.get("in") == "header"]

    rb = op.get("requestBody")
    body_schema = None
    body_content_type = "application/json"
    if rb:
        for ct, media in rb.get("content", {}).items():
            body_schema = media.get("schema", {})
            body_content_type = ct
            break

    # Collect models
    all_models: dict[str, dict] = {}
    if body_schema:
        all_models.update(_collect_models(body_schema, "RequestBody", spec))

    success_schema = None
    for code, resp in op.get("responses", {}).items():
        if code.startswith("2"):
            for ct, media in resp.get("content", {}).items():
                success_schema = media.get("schema", {})
                break
            break
    if success_schema:
        all_models.update(_collect_models(success_schema, "Response", spec))

    # Build function signature parts
    func_name = op.get("operationId", f"{method}_{path.strip('/').replace('/', '_').replace('{','').replace('}','')}")
    func_name = func_name.replace("-", "_")

    sig_parts = []
    for p in path_params:
        pname = p.get("name", "param").replace("-", "_")
        sig_parts.append(f"{pname}: {_py_type(p.get('schema', {}))}")
    for p in query_params:
        pname = p.get("name", "param").replace("-", "_")
        py_t = _py_type(p.get("schema", {}))
        if p.get("required"):
            sig_parts.append(f"{pname}: {py_t}")
        else:
            sig_parts.append(f"{pname}: Optional[{py_t}] = None")
    for p in header_params:
        pname = p.get("name", "header").replace("-", "_")
        sig_parts.append(f"{pname}: Optional[str] = None")
    if body_schema:
        body_model = _model_name("RequestBody") if all_models.get("RequestBody") else "Dict[str, Any]"
        sig_parts.append(f"body: {body_model}")

    return_type = "Any"
    if success_schema:
        if all_models.get("Response"):
            return_type = _model_name("Response")
        else:
            return_type = _py_type(success_schema)

    # Assemble output lines
    lines = []
    lines.append("from __future__ import annotations")
    lines.append("from typing import Any, Dict, List, Optional")
    lines.append("import requests")
    lines.append("from pydantic import BaseModel")
    lines.append("")

    for mname, mschema in all_models.items():
        lines.extend(_generate_pydantic_model(mname, mschema))
        lines.append("")

    sig = f"def {func_name}({', '.join(sig_parts)}) -> {return_type}:"
    lines.append(sig)

    # URL
    url_expr = f'"{base_url}{path}"'
    if path_params:
        fmt_args = ", ".join(f'{p.get("name","p")}={p.get("name","p").replace("-","_")}' for p in path_params)
        url_expr = f'"{base_url}{path}".format({fmt_args})'
    lines.append(f"    url = {url_expr}")

    # Query params dict
    if query_params:
        lines.append("    params = {")
        for p in query_params:
            pname = p.get("name", "param")
            lines.append(f'        "{pname}": {pname.replace("-","_")},')
        lines.append("    }")
    else:
        lines.append("    params: Dict[str, Any] = {}")

    # Headers dict
    if header_params:
        lines.append("    headers = {")
        for p in header_params:
            pname = p.get("name", "header")
            lines.append(f'        "{pname}": {pname.replace("-","_")},')
        lines.append("    }")
    else:
        lines.append("    headers: Dict[str, Any] = {}")

    # Request call
    call_args = ["url", "params=params", "headers=headers"]
    if body_schema:
        if "application/json" in body_content_type:
            call_args.append("json=body.model_dump(exclude_none=True)")
        else:
            call_args.append("data=body.model_dump(exclude_none=True)")
    lines.append(f"    response = requests.{method.lower()}({', '.join(call_args)})")
    lines.append("    response.raise_for_status()")

    # Return
    if return_type == "Any":
        lines.append("    return response.json()")
    elif return_type == _model_name("Response"):
        lines.append(f"    return {return_type}.model_validate(response.json())")
    else:
        lines.append("    return response.json()")

    return "\n".join(lines)


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
        self.source = None
        self.history_list = []
        self.opt_verbose = False
        self.opt_color = True
        self.opt_format = "table"
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
        tags = set()
        for _, _, op in _operations(self.spec):
            for t in op.get("tags", []):
                tags.add(t)
        return sorted(tags)

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
            self.spec = load_spec(source)
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
        if params:
            out("\nParameters:")
            rows = [(p.get("name",""), p.get("in",""), p.get("schema",{}).get("type",""), "yes" if p.get("required") else "no", p.get("description","")) for p in params]
            make_table(["Name", "In", "Type", "Required", "Description"], rows)
        if "requestBody" in op:
            rb = op["requestBody"]
            out(f"\nRequest Body (required={rb.get('required', False)}):")
            for ct, media in rb.get("content", {}).items():
                out(f"  Content-Type: {ct}")
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
        name = arg.strip()
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
        if not params:
            out("No parameters defined.")
            return
        rows = [
            (p.get("name",""), p.get("in",""), p.get("schema",{}).get("type",""),
             "yes" if p.get("required") else "no",
             p.get("description","") if self.opt_verbose else p.get("description","")[:60])
            for p in params
        ]
        make_table(["Name", "In", "Type", "Required", "Description"], rows, title=f"Params: {method.upper()} {path}")

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
        rb = op.get("requestBody")
        if not rb:
            out("No request body defined.")
            return
        header(f"Request Body: {method.upper()} {path}")
        kv("Required", rb.get("required", False))
        for ct, media in rb.get("content", {}).items():
            out(f"\nContent-Type: {ct}")
            schema = media.get("schema", {})
            lines = _resolve_schema(schema)
            if lines:
                rows = [(" " * ind + name, typ, desc) for ind, name, typ, desc in lines]
                make_table(["Property *=required", "Type", "Description"], rows)
            else:
                out(f"  Type: {schema.get('type', 'object')}")

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
        for ct, media in resp.get("content", {}).items():
            out(f"\nContent-Type: {ct}")
            schema = media.get("schema", {})
            lines = _resolve_schema(schema)
            if lines:
                rows = [(" " * ind + name, typ, desc) for ind, name, typ, desc in lines]
                make_table(["Property *=required", "Type", "Description"], rows)
            else:
                out(f"  Type: {schema.get('type','object')}")

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
        kv("Type", schema.get("type","object"))
        if self.opt_verbose and schema.get("description"):
            kv("Description", schema["description"])
        lines = _resolve_schema(schema)
        if lines:
            rows = [(" " * ind + prop, typ, desc) for ind, prop, typ, desc in lines]
            make_table(["Property *=required", "Type", "Description"], rows)
        else:
            out("(no properties)")

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
        raw = _load_raw(self.source)
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
        """search <term>  Full-text search across paths, summaries, descriptions, and schema names."""
        if not self._require_spec():
            return
        term = arg.strip().lower()
        if not term:
            err("Usage: search <term>")
            return
        rows = []
        for path, method, op in _operations(self.spec):
            if (term in path.lower()
                    or term in op.get("summary","").lower()
                    or term in op.get("description","").lower()
                    or term in op.get("operationId","").lower()):
                rows.append(("operation", f"{method.upper()} {path}", op.get("summary","")))
        for name in _schemas(self.spec):
            if term in name.lower():
                rows.append(("schema", name, ""))
        for name, scheme in _security_schemes(self.spec).items():
            if term in name.lower():
                rows.append(("security", name, scheme.get("type","")))
        if rows:
            make_table(["Category", "Match", "Summary"], rows, title=f'Search: "{arg.strip()}"')
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

    def complete_codegen(self, text, line, begidx, endidx):
        return self._complete_path_method(text, line, begidx, endidx)

    def do_codegen(self, arg):
        """codegen <path> <method>  Generate a Python requests + Pydantic snippet for an operation."""
        if not self._require_spec():
            return
        path, method = self._split_path_method(arg)
        if path is None:
            return
        if _find_op(self.spec, path, method) is None:
            err(f"Operation not found: {method.upper()} {path}")
            return
        self._record_history(path, method)
        code = generate_codegen(self.spec, path, method)
        header(f"codegen: {method.upper()} {path}")
        if RICH and _is_color():
            console.print(Syntax(code, "python", theme="monokai", line_numbers=True))
        else:
            print(code)

    def do_history(self, arg):
        """history  Show recently visited operations."""
        if not self.history_list:
            out("No history yet.")
            return
        rows = [(i + 1, method, path) for i, (path, method) in enumerate(self.history_list)]
        make_table(["#", "Method", "Path"], rows, title="History")

    def do_set(self, arg):
        """set <option> <value>  Toggle display options (verbose, color, format)."""
        parts = arg.split()
        if len(parts) != 2:
            err("Usage: set <option> <value>")
            out("Options: verbose on|off  /  color on|off  /  format table|plain")
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
        elif option == "format":
            if value not in ("table","plain"):
                err("format: expected 'table' or 'plain'")
                return
            self.opt_format = value
            out(f"format = {value}")
        else:
            err(f"Unknown option: {option}")
            out("Options: verbose on|off  /  color on|off  /  format table|plain")

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
