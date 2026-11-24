"""Shared helpers that read an Apache Airflow source checkout.

Used by the reference generators and by check_docs.py. Nothing here imports Airflow: the
config template is read as YAML, and the CLI definitions and deprecated-option table are read
with ``ast``, so the tools run without installing Airflow.

Needs Python 3.10+ and PyYAML.
"""

from __future__ import annotations

import ast
import os
import subprocess
from dataclasses import dataclass, field

import yaml

CONFIG_YML = "airflow-core/src/airflow/config_templates/config.yml"
CLI_CONFIG = "airflow-core/src/airflow/cli/cli_config.py"
METRICS_YML = "shared/observability/src/airflow_shared/observability/metrics/metrics_template.yaml"
PARSER_PY = "shared/configuration/src/airflow_shared/configuration/parser.py"
CORE_PYPROJECT = "airflow-core/pyproject.toml"

# CLI groups that providers register at run time (through the executor or auth manager in use),
# so they are not in cli_config.py. check_docs.py doesn't check commands under these.
PROVIDER_CLI_GROUPS = {"celery", "kubernetes", "users", "roles", "permissions", "sync-perm", "fab-db", "edge"}


def read(checkout: str, rel: str) -> str:
    with open(os.path.join(checkout, rel), encoding="utf-8") as f:
        return f.read()


def describe_checkout(checkout: str) -> str:
    """Return 'version X (commit abc)' for the checkout, as far as it can be determined."""
    version = "unknown"
    for line in read(checkout, CORE_PYPROJECT).splitlines():
        if line.startswith("version = "):
            version = line.split("=", 1)[1].strip().strip('"')
            break
    commit = os.environ.get("AIRFLOW_SOURCE_COMMIT", "")
    if not commit:
        try:
            commit = subprocess.run(
                ["git", "-C", checkout, "rev-parse", "--short=10", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            commit = ""
    return f"{version} (commit `{commit}`)" if commit else version


# --- configuration -------------------------------------------------------------------------


def load_config(checkout: str) -> dict:
    """Return the parsed config.yml: {section: {"description": ..., "options": {key: {...}}}}."""
    return yaml.safe_load(read(checkout, CONFIG_YML))


def load_provider_configs(providers_dir: str) -> dict[str, dict]:
    """Return {section: {"options": {...}, "provider": name}} from every providers/**/provider.yaml."""
    out: dict[str, dict] = {}
    if not providers_dir or not os.path.isdir(providers_dir):
        return out
    for d, dirs, files in os.walk(providers_dir):
        dirs[:] = [x for x in dirs if x not in {"tests", "docs", "src", "node_modules", ".git"}]
        if "provider.yaml" not in files:
            continue
        with open(os.path.join(d, "provider.yaml"), encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        for section, body in (data.get("config") or {}).items():
            entry = out.setdefault(section, {"options": {}, "provider": data.get("package-name", d)})
            entry["options"].update((body or {}).get("options") or {})
    return out


def load_deprecated_options(checkout: str) -> dict[tuple[str, str], tuple[str, str, str]]:
    """Return {(new_section, new_key): (old_section, old_key, since_version)} from the config parser."""
    tree = ast.parse(read(checkout, PARSER_PY))
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == "deprecated_options":
            return ast.literal_eval(node.value)
    return {}


# --- CLI -----------------------------------------------------------------------------------


@dataclass
class CliArg:
    flags: tuple[str, ...]
    help: str = ""
    default: str | None = None
    choices: str | None = None
    action: str | None = None
    nargs: str | None = None
    metavar: str | None = None
    required: bool = False

    @property
    def positional(self) -> bool:
        return not self.flags[0].startswith("-")


@dataclass
class CliCommand:
    name: str
    help: str = ""
    description: str = ""
    args: list[CliArg] = field(default_factory=list)
    subcommands: list[CliCommand] = field(default_factory=list)


def _text(node: ast.AST | None) -> str:
    """Best-effort string value of an expression; falls back to its source text."""
    if node is None:
        return ""
    try:
        value = ast.literal_eval(node)
        return value if isinstance(value, str) else repr(value)
    except ValueError:
        pass
    if isinstance(node, ast.JoinedStr):
        parts = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            else:
                parts.append("{" + ast.unparse(v.value) + "}")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _text(node.left) + _text(node.right)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        return _text(node.func.value)
    return ast.unparse(node)


def load_cli(checkout: str) -> list[CliCommand]:
    """Return the core CLI command tree (``core_commands``) from cli_config.py."""
    tree = ast.parse(read(checkout, CLI_CONFIG))
    assigns: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            assigns[node.targets[0].id] = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value:
            assigns[node.target.id] = node.value

    arg_cache: dict[str, CliArg | None] = {}

    def build_arg(name: str) -> CliArg | None:
        if name in arg_cache:
            return arg_cache[name]
        node = assigns.get(name)
        result = None
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Arg":
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            flags_node = node.args[0] if node.args else kw.get("flags")
            try:
                flags = tuple(ast.literal_eval(flags_node))
            except (ValueError, TypeError):
                flags = (ast.unparse(flags_node),)
            result = CliArg(
                flags=flags,
                help=_text(kw.get("help")),
                default=ast.unparse(kw["default"]) if "default" in kw else None,
                choices=ast.unparse(kw["choices"]) if "choices" in kw else None,
                action=_text(kw.get("action")) or None,
                nargs=_text(kw.get("nargs")) or None,
                metavar=_text(kw.get("metavar")) or None,
                required="required" in kw and ast.unparse(kw["required"]) == "True",
            )
        arg_cache[name] = result
        return result

    def args_of(node: ast.AST | None) -> list[CliArg]:
        out: list[CliArg] = []
        if node is None:
            return out
        if isinstance(node, ast.Name) and node.id in assigns and not node.id.startswith("ARG_"):
            return args_of(assigns[node.id])
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return args_of(node.left) + args_of(node.right)
        if isinstance(node, (ast.Tuple, ast.List)):
            for elt in node.elts:
                if isinstance(elt, ast.Starred):
                    out += args_of(elt.value)
                elif isinstance(elt, ast.Name):
                    arg = build_arg(elt.id)
                    if arg:
                        out.append(arg)
        return out

    def commands_of(node: ast.AST | None) -> list[CliCommand]:
        if isinstance(node, ast.Name):
            return commands_of(assigns.get(node.id))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return commands_of(node.left) + commands_of(node.right)
        if not isinstance(node, (ast.Tuple, ast.List)):
            return []
        out = []
        for elt in node.elts:
            if isinstance(elt, ast.Starred):
                out += commands_of(elt.value)
            elif isinstance(elt, ast.Call) and getattr(elt.func, "id", None) in ("ActionCommand", "GroupCommand"):
                kw = {k.arg: k.value for k in elt.keywords if k.arg}
                out.append(
                    CliCommand(
                        name=_text(kw.get("name")),
                        help=_text(kw.get("help")),
                        description=_text(kw.get("description")),
                        args=args_of(kw.get("args")),
                        subcommands=commands_of(kw.get("subcommands")),
                    )
                )
        return out

    return commands_of(assigns["core_commands"])


# --- metrics -------------------------------------------------------------------------------


def load_metrics(checkout: str) -> list[dict]:
    return yaml.safe_load(read(checkout, METRICS_YML))["metrics"]
