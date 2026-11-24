# Contributing

Corrections, new runbooks and new checks are all welcome. This guide stays useful only while it matches the real behavior of Apache Airflow 3.4.x, so every change must keep it **verifiable**.

## Ground rules

1. **Cite the code.** For a claim about a name or a default, link to the Airflow source on the `v3-4-stable` branch (`config.yml`, `cli_config.py`, the metrics registry, the Task SDK) or to the generated pages in `09_reference/`. For a claim about behavior, point to the code that implements it. A claim you can't tie to source is labeled *(not verified against source)*.
2. **Airflow 2 knowledge is not evidence.** Most wrong answers about Airflow 3 come from 2.x habits: `execution_date`, `schedule_interval`, `airflow webserver`, `[webserver]` keys, `airflow.models` imports, direct database access from tasks. Check the 3.4 source every time. <!-- check_docs: ignore -->
3. **Don't edit generated pages by hand.** `09_reference/01_airflow_cfg_reference.md`, `02_cli_reference.md`, `03_metrics_reference.md` and `llms.txt` are generated; see [tools/README.md](tools/README.md).
4. **Write only original text.** Don't paste text from vendor docs, blogs, Stack Overflow or other non-Apache-2.0 sources; paraphrase and link instead. Excerpts from Apache Airflow (Apache-2.0), including its official docs, are fine with attribution, but prefer your own words.
5. **Follow the trademark rules** in [AGENTS.md](AGENTS.md#licensing-and-trademarks-required). Don't present this guide as official ASF documentation.
6. **Keep the disclaimer notice.** Every guide page starts with the `[!CAUTION]` notice under its title that links to the [disclaimer](README.md#disclaimer). Copy it into new pages. Commands that change data, configuration or running deployments need a warning, and must not target production by default.

## How to submit a change

1. **Open an issue first for anything bigger than a fix**, so we can agree where a new page or section goes before you write it. Typos and small corrections can go straight to a pull request.
2. **Make the change** on a branch, following the ground rules above and the [style](AGENTS.md#style) rules.
3. **Run the checks** below. Fix anything that fails.
4. **Open a pull request** and say, for each changed claim, which source file supports it. Reviewers check evidence more than wording.

## Before you open a pull request

You need Python 3.10+ with PyYAML, and the Airflow `v3-4-stable` source:

```bash
git clone --depth 1 --branch v3-4-stable https://github.com/apache/airflow.git ../airflow-3.4
```

Then run:

```bash
python3 tools/check_links.py
python3 tools/check_docs.py ../airflow-3.4
python3 tools/build_ai_bundle.py --check     # if it fails, run it without --check and commit llms.txt
```

The same checks run in CI on every push and pull request, and weekly against the current `v3-4-stable` branch to catch drift ([`.github/workflows/checks.yml`](.github/workflows/checks.yml)).

When you add a page, open it with an **Official docs:** line under the caution notice that links the matching pages on airflow.apache.org (core pages under `/docs/apache-airflow/3.4.0/`).

## Using an AI assistant to contribute

That's fine. Claude Code, Gemini CLI, OpenAI Codex and other tools that read `AGENTS.md` pick up this guide's rules automatically. See [AI_USAGE.md](AI_USAGE.md). You are still responsible for checking the result: AI-written claims must meet the same verification rules as any other change.

## Licensing

By contributing, you agree that your contribution is licensed under the [Apache License 2.0](LICENSE), the same license as the rest of the guide.
