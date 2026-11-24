# AGENTS.md: Instructions for AI Coding Agents

This file is for AI agents working in this folder: OpenAI Codex, Gemini CLI (through [GEMINI.md](GEMINI.md)), Claude Code (through [CLAUDE.md](CLAUDE.md)), Cursor, Copilot and others. People using the guide with a chat assistant should read [AI_USAGE.md](AI_USAGE.md) instead.

## What this folder is

The **Field Guide for Apache Airflow® 3.4** is independent, Apache-2.0 licensed Markdown documentation of runbooks and guidance for Airflow 3.4.x. It is not affiliated with The Apache Software Foundation, and it is not the official Airflow documentation (that lives in `airflow-core/docs/` and at airflow.apache.org).

| Path | Contents | Edit by hand? |
| :--- | :--- | :--- |
| `00_…/` to `08_…/` | Hand-written guide pages, in lifecycle order (`00_getting_started/` is the quickstart) | Yes |
| `09_reference/01_airflow_cfg_reference.md`, `02_cli_reference.md`, `03_metrics_reference.md` | Generated from an Airflow source checkout | **No.** Regenerate them with `tools/` |
| `09_reference/04_…` and later | Hand-written reference pages | Yes |
| `README.md` | Table of contents, version scope, role navigation. The TOC bullets also feed `llms.txt` | Yes |
| `AI_USAGE.md`, `llms.txt` | How to use the guide with AI assistants, and the generated page index | `AI_USAGE.md`: yes. `llms.txt`: no, it is generated |
| `tools/` | Python 3 scripts (standard library plus PyYAML): reference generators, drift checker, link checker, AI bundle builder | Yes |
| `LICENSE`, `NOTICE` | Apache-2.0 text and attribution | Only to add attribution |

## Two kinds of task

### Answering questions from the guide
Follow the rules in [AI_USAGE.md §2](AI_USAGE.md#2-rules-for-assistants-answering-from-this-guide):
- Scope is Airflow 3.4.x. Airflow 2.x answers are often wrong for 3.x (no direct metadata-DB access from tasks, no `execution_date`, no SubDags, `catchup` defaults to `False`).
- Take exact config keys, CLI commands and flags, and metric names from `09_reference/`. Never invent a key, flag, metric or default.
- Cite the page and section you got each answer from.
- Warn before irreversible operations (`airflow db clean`, `airflow db downgrade`, `airflow db reset`, deleting Dags, rotating the Fernet key) and mention the known traps listed there.

### Changing the guide
1. **Check facts before you write them.** These are the arbiters:
   - **Names and defaults:** the Apache Airflow source on the `v3-4-stable` branch: `airflow-core/src/airflow/config_templates/config.yml`, `airflow-core/src/airflow/cli/cli_config.py`, `shared/observability/src/airflow_shared/observability/metrics/metrics_template.yaml`, the Task SDK in `task-sdk/src/airflow/sdk/`, and the Helm chart in `chart/values.yaml`.
   - **Behavior:** the source code that implements it. Cite the file. A claim you could not tie to source is labeled *(not verified against source)*.
   - Your memory, blog posts and other vendors' docs don't decide anything. Airflow 2.x material is the most common source of wrong answers. When this guide and the official docs disagree, say which one the source supports.
2. **Run the static checks before you finish.** All of them must pass:
   ```bash
   python3 tools/check_links.py                           # relative links and #anchors, caution notice
   python3 tools/check_docs.py <airflow-checkout>         # CLI commands, flags and [section] keys exist
   python3 tools/build_ai_bundle.py --check               # llms.txt matches the README TOC
   ```
   Inside the Airflow repository, run them with `uv run --project scripts python …` rather than a bare `python3`.
3. **Adding, renaming or removing a page:**
   - Update the TOC bullet and phase in `README.md`, and the role table if it applies.
   - Fix every link that pointed to the old path.
   - Run `python3 tools/build_ai_bundle.py` and commit the new `llms.txt`.
4. **New Airflow 3.4.x release:** regenerate the three generated reference pages against the new tag, read the release notes (`RELEASE_NOTES.rst`) and `airflow-core/newsfragments/*.significant.rst`, run the checks, and update the version-scope line in `README.md`.

## Style

* Write in plain US English for working engineers. Prefer short sentences, and make each step something the reader can act on.
* Write **Dag** in prose. Keep `DAG` only for the literal Python class (`from airflow.sdk import DAG`), and keep CLI and identifier spellings (`airflow dags list`, `dag_id`) as they are.
* Number top-level sections (`## 1. Title`). Anchors such as `#3-dag-processor` are linked from other pages, so check for incoming links before you rename a heading.
* Use GitHub alerts (`> [!IMPORTANT]`, `> [!WARNING]`, `> [!NOTE]`, `> [!TIP]`) for irreversible or surprising behavior.
* Use tables with `:---` alignment. Tag code blocks with a language (`bash`, `python`, `ini`, `yaml`, `sql`).
* Write config keys as `[section] key` in prose (for example `[dag_processor] parsing_processes`). Several keys moved section between 2.x and 3.x, so take the section from `09_reference/01_airflow_cfg_reference.md`. Give the environment variable form `AIRFLOW__SECTION__KEY` where readers will need it.
* Import Dag-authoring names from `airflow.sdk` (`from airflow.sdk import DAG, dag, task, Asset`), the Airflow 3 public interface. Don't use `airflow.models`, `airflow.decorators` or `airflow.datasets` in examples.
* Label performance numbers and sizing figures as illustrative.
* Every guide page starts with the `[!CAUTION]` notice under its title that links to `README.md#disclaimer`. Keep it, and copy it into new pages. The generators write it on the generated pages.
* Commands must run as written. Don't put placeholders where a reader would paste them unchanged; use `<angle_brackets>` and say what to substitute.

## Licensing and trademarks: required

* All content is Apache-2.0. Don't paste text from sources under other licenses, for example vendor documentation, Stack Overflow (CC BY-SA), or blog posts. Paraphrase and link instead. Excerpts from Apache Airflow itself are fine, with attribution, because it is Apache-2.0.
* Keep the attribution header that the generators write on generated pages, and keep `NOTICE` intact.
* Trademarks: use "Apache Airflow®" on first use in the README. Don't use the Airflow logo (the pinwheel). Don't name the project or site in a way that suggests it is official. Don't claim the ASF endorses the guide.
* Don't add personal data, real hostnames, IPs other than RFC 5737/1918 example ranges, or real credentials.
