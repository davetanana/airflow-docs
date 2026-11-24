# CLAUDE.md

@AGENTS.md

## Claude Code specifics

- This folder lives inside the Apache Airflow repository. The repository's own `CLAUDE.md` still applies: don't run `python`, `pytest` or `airflow` directly on the host. Run the guide's tools with `uv run --project scripts python new_docs/tools/<tool>.py` from the repository root, or with `python3` from a standalone copy of the guide.
- Read the 3.4 source with `git show origin/v3-4-stable:<path>`, or extract it once with `git archive origin/v3-4-stable airflow-core shared task-sdk chart | tar -x -C <dir>`, and pass that directory to the tools.
