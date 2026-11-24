#!/usr/bin/env python3
"""Build the files that make this guide easy to give to an AI assistant.

Usage:
  python3 tools/build_ai_bundle.py [--base-url URL] [--check]

Writes:
  llms.txt                               index of every page with a one-line summary (llmstxt.org format).
                                         Committed. Links are relative unless --base-url is given (then it
                                         is written to dist/llms.txt instead, for publishing).
  dist/field-guide-airflow-3.4.md        every hand-written page in reading order, in one file, with the
                                         usage rules from AI_USAGE.md at the top. For chat uploads.
  dist/field-guide-airflow-3.4-reference.md   the generated reference pages, in one file.

--check exits 1 if llms.txt is out of date. Page summaries come from the Table of Contents
bullets in README.md, so keep those current. Standard library only.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOC_ITEM = re.compile(r"^\* \*\*\[[^\]]+\]\(([^)]+\.md)\)\*\*:\s*(.+)$")
PHASE = re.compile(r"^### (Phase \d+: .+)$")
GENERATED = (
    "09_reference/01_airflow_cfg_reference.md",
    "09_reference/02_cli_reference.md",
    "09_reference/03_metrics_reference.md",
)
NAME = "field-guide-airflow-3.4"


def toc() -> list[tuple[str, list[tuple[str, str]]]]:
    phases: list[tuple[str, list[tuple[str, str]]]] = []
    cur = None
    with open(os.path.join(ROOT, "README.md"), encoding="utf-8") as f:
        for line in f:
            m = PHASE.match(line.strip())
            if m:
                cur = (m.group(1), [])
                phases.append(cur)
                continue
            m = TOC_ITEM.match(line.strip())
            if m and cur:
                cur[1].append((m.group(1), m.group(2)))
    return phases


def title(path: str) -> str:
    with open(os.path.join(ROOT, path), encoding="utf-8") as f:
        for line in f:
            if line.startswith("# "):
                return line[2:].strip()
    return path


def llms_txt(base: str) -> str:
    out = [
        "# Field Guide for Apache Airflow 3.4",
        "",
        "> Independent, opinionated runbooks and guidance for running and authoring for Apache Airflow 3.4.x: "
        "architecture, Dag authoring, scheduling and assets, executors, deferrable tasks, concurrency, deployment, "
        "Kubernetes and the Helm chart, configuration, security, upgrading from Airflow 2, day-2 operations, "
        "monitoring, logging, troubleshooting and disaster recovery. "
        "Not affiliated with or endorsed by The Apache Software Foundation.",
        "",
        f"Read [AI_USAGE.md]({base}AI_USAGE.md) first: it gives the rules for answering from this guide "
        "(version scope, Airflow 2 vs 3 differences, irreversible operations, and how to cite). Names and defaults "
        "were checked against the Airflow 3.4 source.",
        "",
    ]
    optional = []
    for phase, items in toc():
        section = []
        for path, summary in items:
            line = f"- [{title(path)}]({base}{path}): {summary}"
            (optional if path in GENERATED else section).append(line)
        if section:
            out += [f"## {phase}", ""] + section + [""]
    out += (
        ["## Optional", ""]
        + optional
        + [f"- [Documentation tools]({base}tools/README.md): reference generators and drift checker.", ""]
    )
    return "\n".join(out)


def bundle(paths: list[str], head: str) -> str:
    parts = [head]
    for p in paths:
        with open(os.path.join(ROOT, p), encoding="utf-8") as f:
            parts.append(f"\n\n---\n\n<!-- source: {p} -->\n\n" + f.read().rstrip() + "\n")
    return "".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--base-url", default="", help="prefix for links in llms.txt (default: relative)")
    ap.add_argument("--check", action="store_true", help="exit 1 if llms.txt is stale; write nothing")
    a = ap.parse_args()
    base = a.base_url if not a.base_url or a.base_url.endswith("/") else a.base_url + "/"

    txt = llms_txt(base)
    llms = os.path.join(ROOT, "llms.txt")
    if a.check:
        cur = open(llms, encoding="utf-8").read() if os.path.exists(llms) else ""
        if cur != txt:
            print("llms.txt is out of date: run python3 tools/build_ai_bundle.py")
            return 1
        print("llms.txt is up to date")
        return 0
    os.makedirs(os.path.join(ROOT, "dist"), exist_ok=True)
    out_llms = "dist/llms.txt" if a.base_url else "llms.txt"
    with open(os.path.join(ROOT, out_llms), "w", encoding="utf-8") as f:
        f.write(txt)

    pages = [p for _, items in toc() for p, _ in items]
    guide = [p for p in pages if p not in GENERATED]
    with open(os.path.join(ROOT, "AI_USAGE.md"), encoding="utf-8") as f:
        usage = f.read().rstrip()
    head = (
        "# Field Guide for Apache Airflow 3.4 (single-file edition)\n\n"
        "Generated by tools/build_ai_bundle.py. Each page starts after a `<!-- source: path -->` marker; "
        "cite that path and the section heading.\n\n" + usage + "\n"
    )
    with open(os.path.join(ROOT, "dist", f"{NAME}.md"), "w", encoding="utf-8") as f:
        f.write(bundle(["README.md"] + guide, head))
    with open(os.path.join(ROOT, "dist", f"{NAME}-reference.md"), "w", encoding="utf-8") as f:
        f.write(bundle(list(GENERATED), "# Field Guide for Apache Airflow 3.4: generated reference\n"))
    for p in (out_llms, f"dist/{NAME}.md", f"dist/{NAME}-reference.md"):
        print(f"wrote {p} ({os.path.getsize(os.path.join(ROOT, p)) // 1024} KiB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
