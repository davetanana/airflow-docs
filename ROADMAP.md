# Roadmap

Where this guide stands against the official Apache Airflow 3.4 documentation, and what to work on next. Pick anything here; open an issue first for larger items (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## 1. How the guide compares with the official docs

The official docs source on the `v3-4-stable` branch (`airflow-core/docs`, 159 reStructuredText pages, about 250,000 words, checked November 2026) is the parameter-by-parameter and concept reference. This guide aims to complement it, not replace it: it should link to the official pages for theory and full API detail, and own the operational "how do I do this safely" material.

| Area | Official 3.4 docs | This guide |
| :--- | :--- | :--- |
| Organization | By concept and feature | By the order a team meets Airflow, from a first Dag to disaster recovery |
| Troubleshooting | One troubleshooting page plus FAQ | Organized by symptom, with the exact log text from the source |
| Concurrency | Separate pages for pools, priority, config | One page with every knob in the order the scheduler applies them |
| Upgrading 2 → 3 | Detailed upgrade page | A runbook with rollback limits and the traps that bite |
| Accuracy | Hand-written | Names and defaults checked against the 3.4 source by `tools/check_docs.py`; three reference pages generated from source |
| **Providers** | Each provider has its own docs | Covered only where operations need them (Celery, Kubernetes, remote logging, secrets backends) |
| **Breadth** | Plugins, listeners, cluster policies, UI customization, lineage, the full REST API | Light or missing |

## 2. Content gaps, in priority order

1. **A live validation environment.** The Cassandra field guide this one is modeled on runs its claims on a live cluster (`test_env/`). This guide doesn't yet: names and defaults are checked against source, but behavior is not run. A `test_env/` with the official Docker Compose file (or `breeze`), and scenarios that run the quickstart, the tutorials, `airflow db clean`, a backfill, a Fernet key rotation, and the troubleshooting log texts, would close the largest gap.
2. **A system-table reference.** One page per metadata table that operators query or clean (`dag_run`, `task_instance`, `task_instance_history`, `xcom`, `log`, `job`, `dag_version`, `serialized_dag`), generated from the SQLAlchemy models.
3. **Plugins, listeners and cluster policies** as operational tools (enforcing owners, tags, pools or queues across all Dags).
4. **Managed services.** Notes on what changes on hosted Airflow offerings, kept vendor-neutral.
5. **A provider defaults page** for the Celery and Kubernetes executors, generated from each provider's `provider.yaml`, like the core config reference.
6. **Glossary.** Dag run, task instance, logical date, data interval, run_after, asset event, bundle, Dag version, workload, triggerer, deferral.

## 3. Project health

* **Release cadence.** For each Airflow 3.4.x release: regenerate `09_reference/` against the new tag, read the significant newsfragments, rerun the checks, and update the version line in the README.
* **Airflow 3.5.** When 3.5 ships, decide whether to branch the guide per minor version or add version notes to each page.
