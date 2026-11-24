#!/usr/bin/env python3
"""Drift checker: find CLI commands, flags and config keys the guide uses but Airflow doesn't define.

Usage:
  python3 tools/check_docs.py <airflow-checkout> [--providers DIR] [--strict]

Errors (exit 1):
  * `airflow <cmd> [<sub>]` in inline code or a shell code block where the command or
    subcommand isn't in cli_config.py (provider groups such as `airflow celery` are skipped).
  * A `--flag` or `-f` after such a command that the command doesn't define.
  * A `[section] key`, `AIRFLOW__SECTION__KEY`, or `key = value` under `[section]` in an ini
    block, where the key isn't in config.yml or in a provider's provider.yaml.
Warnings (exit 1 only with --strict):
  * A key that is a deprecated alias, such as `[scheduler] dag_dir_list_interval`.
  * A section that is neither core nor in a provider found under --providers.

A line that contains <!-- check_docs: ignore --> is skipped: use it where a page names a removed
2.x command or key on purpose.

--providers defaults to <checkout>/providers when it exists. Needs Python 3.10+ and PyYAML.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import airflow_source as src  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FENCE = re.compile(r"^\s*(```|~~~)\s*([\w+-]*)")
SHELL_LANGS = {"bash", "sh", "shell", "console", "text", "zsh", ""}
INI_LANGS = {"ini", "cfg"}
SEP = {"|", "||", "&&", ";", "\\", ">", ">>", "2>&1", "&"}
KEY_REF = re.compile(r"`?\[([a-z][a-z0-9_]*)\]`?\s+`([a-z][a-z0-9_]*)`|`\[([a-z][a-z0-9_]*)\]\s+([a-z][a-z0-9_]*)`")
ENV_REF = re.compile(r"\bAIRFLOW__([A-Z][A-Z0-9_]*?)__([A-Z][A-Z0-9_]*)\b")
# Put this HTML comment on a line that deliberately names something Airflow doesn't define,
# for example a removed 2.x command in an upgrade note.
IGNORE = "<!-- check_docs: ignore -->"
GENERATED = {"09_reference/01_airflow_cfg_reference.md", "09_reference/02_cli_reference.md"}


class Checker:
    def __init__(self, checkout: str, providers: str | None):
        self.cli = {c.name: c for c in src.load_cli(checkout)}
        cfg = src.load_config(checkout)
        self.keys = {s: set((b or {}).get("options") or {}) for s, b in cfg.items()}
        for s, b in src.load_provider_configs(providers).items():
            self.keys.setdefault(s, set()).update(b["options"])
        self.has_providers = bool(providers and os.path.isdir(providers))
        self.old = {(o[0], o[1]): (n, o[2]) for n, o in src.load_deprecated_options(checkout).items()}
        self.errors: list[str] = []
        self.warnings: list[str] = []

    # --- CLI ---
    def check_command(self, where: str, tokens: list[str]) -> None:
        if not tokens or tokens[0] in src.PROVIDER_CLI_GROUPS or tokens[0].startswith("-"):
            return
        name = tokens[0]
        if not re.fullmatch(r"[a-z][a-z-]*", name):
            return
        cmd = self.cli.get(name)
        if cmd is None:
            self.errors.append(f"{where}: unknown command `airflow {name}`")
            return
        rest = tokens[1:]
        if cmd.subcommands:
            if not rest or not re.fullmatch(r"[a-z][a-z-]*", rest[0]):
                return
            sub = next((s for s in cmd.subcommands if s.name == rest[0]), None)
            if sub is None:
                self.errors.append(f"{where}: unknown subcommand `airflow {name} {rest[0]}`")
                return
            cmd, rest, label = sub, rest[1:], f"airflow {name} {rest[0]}"
        else:
            label = f"airflow {name}"
        known = {f for a in cmd.args for f in a.flags} | {"-h", "--help"}
        for t in rest:
            if t == "--":
                break
            if t.startswith("-") and len(t) > 1 and not re.fullmatch(r"-\d.*", t):
                flag = t.split("=", 1)[0].rstrip(").,;:")
                if flag not in known:
                    self.errors.append(f"{where}: `{label}` has no flag `{flag}`")

    def scan_shell(self, where: str, text: str) -> None:
        text = re.sub(r"\s+#.*$", "", text)
        try:
            tokens = shlex.split(text, posix=True)
        except ValueError:
            tokens = text.split()
        i = 0
        while i < len(tokens):
            if tokens[i] == "airflow":
                j = i + 1
                while j < len(tokens) and tokens[j] not in SEP:
                    j += 1
                self.check_command(where, tokens[i + 1 : j])
                i = j
            i += 1

    # --- config ---
    def check_key(self, where: str, section: str, key: str) -> None:
        if section == "section":
            return
        if (section, key) in self.old:
            (ns, nk), ver = self.old[(section, key)]
            self.warnings.append(f"{where}: `[{section}] {key}` is a deprecated alias of `[{ns}] {nk}` ({ver})")
            return
        if section not in self.keys:
            msg = f"{where}: unknown config section `[{section}]`"
            (self.errors if self.has_providers else self.warnings).append(msg)
            return
        if key not in self.keys[section]:
            self.errors.append(f"{where}: `[{section}]` has no key `{key}`")

    def check_env(self, where: str, section: str, key: str) -> None:
        for suffix in ("_CMD", "_SECRET"):
            if key.endswith(suffix) and key[: -len(suffix)].lower() in self.keys.get(section.lower(), set()):
                key = key[: -len(suffix)]
        self.check_key(where, section.lower(), key.lower())

    def scan_prose(self, where: str, line: str) -> None:
        for m in KEY_REF.finditer(line):
            section, key = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
            self.check_key(where, section, key)
        for m in ENV_REF.finditer(line):
            if not line[m.end() : m.end() + 1] == "<":
                self.check_env(where, m.group(1), m.group(2))
        for code in re.findall(r"`([^`]+)`", line):
            if code.startswith("airflow "):
                self.scan_shell(where, code)

    def scan_page(self, rel: str, text: str) -> None:
        lang = None
        ini_section = None
        for no, line in enumerate(text.splitlines(), 1):
            where = f"{rel}:{no}"
            m = FENCE.match(line)
            if m:
                if lang is None:
                    lang, ini_section = m.group(2).lower(), None
                else:
                    lang = None
                continue
            if IGNORE in line:
                continue
            if lang is None:
                self.scan_prose(where, line)
                continue
            for env in ENV_REF.finditer(line):
                if not line[env.end() : env.end() + 1] == "<":
                    self.check_env(where, env.group(1), env.group(2))
            if lang in SHELL_LANGS:
                self.scan_shell(where, line.lstrip("$ "))
            elif lang in INI_LANGS:
                sm = re.match(r"^\s*\[([a-z][a-z0-9_.]*)\]\s*$", line)
                if sm:
                    ini_section = sm.group(1)
                    continue
                km = re.match(r"^\s*([a-z][a-z0-9_]*)\s*=", line)
                if km and ini_section:
                    self.check_key(where, ini_section, km.group(1))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("checkout")
    ap.add_argument("--providers", help="providers/ directory (default: <checkout>/providers)")
    ap.add_argument("--strict", action="store_true", help="warnings fail too")
    a = ap.parse_args()
    providers = a.providers or os.path.join(a.checkout, "providers")
    ck = Checker(a.checkout, providers if os.path.isdir(providers) else None)
    pages = 0
    for d, dirs, files in os.walk(ROOT):
        dirs[:] = [x for x in dirs if x not in {".git", "dist", "tools", "__pycache__"}]
        for f in sorted(files):
            rel = os.path.relpath(os.path.join(d, f), ROOT).replace(os.sep, "/")
            if not f.endswith(".md") or rel in GENERATED:
                continue
            with open(os.path.join(d, f), encoding="utf-8") as fh:
                ck.scan_page(rel, fh.read())
            pages += 1
    for e in sorted(set(ck.errors)):
        print(f"ERROR {e}")
    for w in sorted(set(ck.warnings)):
        print(f"WARN  {w}")
    print(f"{pages} page(s) checked: {len(set(ck.errors))} error(s), {len(set(ck.warnings))} warning(s)")
    if not ck.has_providers:
        print("note: no providers/ directory given, so provider config sections were only warned about")
    return 1 if ck.errors or (a.strict and ck.warnings) else 0


if __name__ == "__main__":
    sys.exit(main())
