#!/usr/bin/env python3
"""Check relative links and #anchors in every Markdown file of the guide.

Usage: python3 tools/check_links.py [--docs DOCS_DIR]

External (http/https/mailto) links are not fetched. A relative link must point to an existing
file or directory, and a #fragment must match a heading (GitHub anchor rules) or an
<a id="..."> in the target page. It also checks that every guide page in the numbered folders
has the [!CAUTION] notice linking to README.md#disclaimer under its title.
Exits 1 if anything is wrong. Standard library only.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

LINK = re.compile(r'(?<!\!)\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)')
FENCE = re.compile(r"^\s*(```|~~~)")
SKIP_DIRS = {".git", "node_modules", "dist", "build", "__pycache__"}
_anchor_cache: dict[str, set[str]] = {}


def slug(heading: str) -> str:
    h = heading.strip().lower()
    h = re.sub(r"<[^>]+>", "", h)
    h = re.sub(r"[^\w\- ]", "", h)
    return h.replace(" ", "-")


def anchors(path: str) -> set[str]:
    if path not in _anchor_cache:
        seen: dict[str, int] = {}
        out: set[str] = set()
        fenced = False
        with open(path, encoding="utf-8") as f:
            for line in f:
                if FENCE.match(line):
                    fenced = not fenced
                    continue
                m = None if fenced else re.match(r"^#{1,6}\s+(.*?)\s*#*\s*$", line)
                if m:
                    s = slug(m.group(1))
                    n = seen.get(s, 0)
                    seen[s] = n + 1
                    out.add(s if n == 0 else f"{s}-{n}")
                out.update(re.findall(r'<a\s+(?:name|id)="([^"]+)"', line))
        _anchor_cache[path] = out
    return _anchor_cache[path]


def md_files(root: str):
    for d, dirs, files in os.walk(root):
        dirs[:] = [x for x in dirs if x not in SKIP_DIRS]
        for f in files:
            if f.endswith(".md"):
                yield os.path.join(d, f)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--docs", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    args = ap.parse_args()
    broken = checked = 0
    for page in sorted(md_files(args.docs)):
        fenced = False
        rel = os.path.relpath(page, args.docs)
        with open(page, encoding="utf-8") as f:
            for no, line in enumerate(f, 1):
                if FENCE.match(line):
                    fenced = not fenced
                    continue
                if fenced:
                    continue
                for target in LINK.findall(re.sub(r"`[^`]*`", "", line)):
                    if re.match(r"^[a-z][a-z0-9+.-]*:", target, re.I):
                        continue
                    checked += 1
                    path, _, frag = target.partition("#")
                    dest = os.path.normpath(os.path.join(os.path.dirname(page), path)) if path else page
                    if not os.path.exists(dest):
                        print(f"{rel}:{no}: missing target {target}")
                        broken += 1
                    elif frag and dest.endswith(".md") and frag not in anchors(dest):
                        print(f"{rel}:{no}: no heading for #{frag} in {os.path.relpath(dest, args.docs)}")
                        broken += 1
    print(f"{checked} relative link(s) checked, {broken} broken")
    missing = []
    for page in sorted(md_files(args.docs)):
        rel = os.path.relpath(page, args.docs).replace(os.sep, "/")
        if re.match(r"\d\d_[^/]+/[^/]+\.md$", rel):
            with open(page, encoding="utf-8") as f:
                head = "".join(f.readlines()[:8])
            if "README.md#disclaimer" not in head:
                missing.append(rel)
    for rel in missing:
        print(f"{rel}: no [!CAUTION] notice linking to README.md#disclaimer under the title")
    return 1 if broken or missing else 0


if __name__ == "__main__":
    sys.exit(main())
