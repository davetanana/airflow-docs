# Tools

Scripts that keep this guide accurate and help readers use it. Run them from the guide's root folder. They read files and print results; none of them connects to an Airflow deployment, imports Airflow, or changes anything outside this folder.

| Tool | For | Needs |
| :--- | :--- | :--- |
| [`check_links.py`](#check_linkspy) | Contributors: relative links, `#anchors`, and the caution notice | Python 3 |
| [`check_docs.py`](#check_docspy) | Contributors: CLI commands and flags, and config keys, exist in Airflow | Python 3.10+, PyYAML, an Airflow checkout |
| [`gen_reference.py`](#gen_referencepy) | Maintainers: regenerate the three generated reference pages | Python 3.10+, PyYAML, an Airflow checkout |
| [`build_ai_bundle.py`](#build_ai_bundlepy) | Maintainers: `llms.txt` and the single-file AI bundle | Python 3 |
| `airflow_source.py` | Shared parsing helpers for the generator and `check_docs.py` | |

Several tools read an Airflow source checkout. Use the branch the guide describes:

```bash
git clone --depth 1 --branch v3-4-stable https://github.com/apache/airflow.git ../airflow-3.4
```

Inside an existing Airflow clone you can export the branch instead, without switching your working tree:

```bash
mkdir -p ../airflow-3.4 && git archive origin/v3-4-stable airflow-core shared task-sdk chart | tar -x -C ../airflow-3.4
export AIRFLOW_SOURCE_COMMIT=$(git rev-parse --short=10 origin/v3-4-stable)   # an export has no .git to read
```

> [!NOTE]
> If this folder sits inside the Airflow repository, follow that repository's rule of not running a bare `python` on the host. Run the tools in a throwaway environment instead: `uv run --no-project --with pyyaml python tools/<tool>.py …`.

---

## `check_links.py`

Checks every relative link and `#anchor` in the Markdown pages, using GitHub's anchor rules and `<a id="…">` targets.

```bash
python3 tools/check_links.py               # or --docs <dir> to check another copy
```

It also checks that every guide page in the numbered folders has the `[!CAUTION]` notice linking to the [disclaimer](../README.md#disclaimer) under its title. Exit code 1 if anything is wrong.

## `check_docs.py`

Drift checker: reports CLI commands and flags, and config keys, that the guide uses but the given Airflow checkout doesn't define.

```bash
python3 tools/check_docs.py ../airflow-3.4 --providers ../airflow/providers        # exit 1 on errors
python3 tools/check_docs.py ../airflow-3.4 --providers ../airflow/providers --strict   # warnings fail too
```

* **Errors**
  * `airflow <command> [<subcommand>]` in inline code or a shell code block (`bash`, `sh`, `console`, `text` or untagged) where the command or subcommand isn't in `cli_config.py`.
  * A flag after such a command that the command doesn't define.
  * A config key that isn't in `config.yml` or in any provider's `provider.yaml`, written as `` `[section] key` ``, `` [section] `key` ``, `AIRFLOW__SECTION__KEY` (with or without `_CMD` / `_SECRET`), or `key = value` under a `[section]` header in an `ini` block.
* **Warnings**
  * A key written with its pre-3.x or otherwise deprecated name (from the `deprecated_options` table in the config parser).
  * A config section that is not core, when no `--providers` directory was given.

A line containing `<!-- check_docs: ignore -->` is skipped; use it where a page names a removed 2.x command or key on purpose, or quotes a process title.

Provider CLI groups (`airflow celery`, `airflow kubernetes`, `airflow users` and others in `PROVIDER_CLI_GROUPS`) are not checked, because providers register them at run time. The generated reference pages are skipped.

## `gen_reference.py`

Regenerates the three generated pages in [`09_reference/`](../09_reference/):

| Page | Source |
| :--- | :--- |
| [`01_airflow_cfg_reference.md`](../09_reference/01_airflow_cfg_reference.md) | `airflow-core/src/airflow/config_templates/config.yml`, plus the renamed-key table in `shared/configuration/.../parser.py` |
| [`02_cli_reference.md`](../09_reference/02_cli_reference.md) | `airflow-core/src/airflow/cli/cli_config.py` (read with `ast`, not imported) |
| [`03_metrics_reference.md`](../09_reference/03_metrics_reference.md) | `shared/observability/.../metrics/metrics_template.yaml` |

```bash
python3 tools/gen_reference.py ../airflow-3.4                 # all three
python3 tools/gen_reference.py ../airflow-3.4 --only cli      # one page
```

Each page records the Airflow version and commit it was built from. Commit the result, and don't edit these pages by hand.

## `build_ai_bundle.py`

Builds the files for using the guide with AI assistants (see [AI_USAGE.md](../AI_USAGE.md)).

```bash
python3 tools/build_ai_bundle.py            # writes llms.txt and dist/
python3 tools/build_ai_bundle.py --check    # exit 1 if llms.txt is stale
python3 tools/build_ai_bundle.py --base-url https://raw.githubusercontent.com/<owner>/<repo>/main/
```

Page summaries in `llms.txt` come from the Table of Contents bullets in [README.md](../README.md), so update the README first when you add or rename a page.
