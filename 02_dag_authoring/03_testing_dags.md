# Testing Dags

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Best Practices](https://airflow.apache.org/docs/apache-airflow/3.4.0/best-practices.html) · [Debugging Airflow Dags](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/debug.html)

The official docs show a few test snippets. This page is a complete test strategy for Airflow 3.4 Dags: which layer catches which bug, the 3.4-correct way to write each test (several Airflow 2 recipes no longer work: `DagBag(include_examples=False)`, `ti.run()`, tasks that query the ORM), what each CLI test command needs from the metadata database, and a CI recipe. Behavior was checked in `airflow-core/src/airflow/dag_processing/dagbag.py`, `task-sdk/src/airflow/sdk/definitions/dag.py` (`DAG.test`) and `airflow-core/src/airflow/cli/commands/` on `v3-4-stable`.

---

## 1. The testing pyramid for Dags

| Layer | Catches | Needs a metadata DB? | Speed |
| :--- | :--- | :--- | :--- |
| Lint (`ruff --select AIR`) | Airflow 2 imports and arguments, runtime-varying values | No | Seconds |
| Import / integrity test (`DagBag`) | Import errors, cycles, invalid trigger rules, bad params, policy violations, duplicate ids | No | Seconds |
| Unit tests of task functions and operators | Business logic, templating, deferral | No | Seconds |
| `dag.test()` / `airflow dags test` | Wiring between tasks, XCom passing, branching, trigger rules, callbacks | **Yes** (SQLite is fine) | Seconds to minutes |
| Staging deployment | Executors, bundles, secrets backends, permissions | Yes | Minutes |

Write many of the first three, a few of the fourth, and run the fifth on every release.

---

## 2. Import and integrity test

The Dag processor uses `DagBag` from `airflow.dag_processing.dagbag` (the old `airflow.models.dagbag` import still works but emits a deprecation warning). `DagBag` is an internal class of `apache-airflow-core`, so pin your Airflow version in the test environment.

```python
# tests/test_dag_integrity.py
from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, "\n".join(f"{f}: {e}" for f, e in dagbag.import_errors.items())


def test_dags_found(dagbag):
    assert dagbag.size() > 0


@pytest.mark.parametrize("dag_id", DagBag(dag_folder=DAGS_DIR).dag_ids)
def test_dag_conventions(dagbag, dag_id):
    dag = dagbag.dags[dag_id]
    assert dag.tags, f"{dag_id} has no tags"
    assert dag.owner != "airflow", f"{dag_id} uses the default owner"
    for t in dag.tasks:
        assert t.retries >= 1, f"{dag_id}.{t.task_id} has no retries"
        assert t.execution_timeout is not None, f"{dag_id}.{t.task_id} has no execution_timeout"
```

What lands in `import_errors` (`DagBag._process_file` and `bag_dag`): exceptions during import, import timeouts (`[core] dagbag_import_timeout`), `dag.validate()` failures (for example `fail_fast` with a non-default trigger rule, invalid params, an AND asset schedule with `batch_asset_events=False`), cycles found by `dag.check_cycle()`, executor names that are not configured, cluster policy errors, and duplicate `dag_id`s.

3.4 specifics:

* `DagBag.__init__` has **no `include_examples` argument** in 3.4; passing it raises `TypeError`. Example Dags are a separate bundle, so a bag built on your folder never includes them.
* `safe_mode` defaults to `[core] dag_discovery_safe_mode` (`True`): files whose lower-cased bytes do not contain `airflow` and at least one of `dag` or `asset` are skipped (`might_contain_dag_via_default_heuristic` in `shared/module_loading`). A Dag built only by a helper module may need `safe_mode=False`.
* Dag files that call `Variable.get()` at import time are served, in a plain test process, by environment variables and any configured `[secrets] backend` only, not by the metadata DB (`ensure_secrets_backend_loaded` fallback chain in `task-sdk/src/airflow/sdk/execution_time/supervisor.py`). Set `AIRFLOW_VAR_*` (§4) or, better, move the call into a task.
* `DagBag` does not serialize Dags. The Dag processor does, and a Dag can import cleanly but fail serialization. `dag.test()` (§5) serializes, so one `dag.test()` per Dag in CI closes that gap.

---

## 3. Unit-testing tasks and operators

**TaskFlow functions.** The decorated object keeps the plain function in `.function`:

```python
from dags.orders_etl import transform  # a module-level @task function


def test_transform_moves_to_staged():
    assert transform.function("s3://example-bucket/raw/orders/2026-01-01/") == (
        "s3://example-bucket/staged/orders/2026-01-01/"
    )
```

Better still, keep logic in a plain module (`include/orders.py`) and make the `@task` a thin wrapper. Then tests import no Airflow at all.

**Custom operators and sensors.** Instantiate them and call `execute()` / `poke()` with only the context keys your code reads. No Dag, no DB:

```python
import pytest

from airflow.sdk.exceptions import TaskDeferred
from my_company.operators import ExportOperator, ExportTrigger


def test_export_execute():
    op = ExportOperator(task_id="export", table="orders")
    assert op.execute(context={}) == "orders.parquet"


def test_export_renders_templates():
    op = ExportOperator(task_id="export", table="orders_{{ ds_nodash }}")
    op.render_template_fields(context={"ds_nodash": "20260101"})
    assert op.table == "orders_20260101"


def test_export_defers():
    op = ExportOperator(task_id="export", table="orders", deferrable=True)
    with pytest.raises(TaskDeferred) as exc:
        op.execute(context={})
    assert isinstance(exc.value.trigger, ExportTrigger)
    assert getattr(op, exc.value.method_name)(context={}, event={"status": "success"}) == "orders.parquet"
```

`TaskInstance.run()` and `TaskInstance.render_templates()` no longer exist; use `op.execute(context)` and `op.render_template_fields(context)`. `TaskDeferred` and `AirflowSkipException` come from `airflow.sdk.exceptions`. Mock hooks with `unittest.mock.patch.object(MyHook, "get_conn", autospec=True)` rather than real connections.

---

## 4. Mocking Variables and Connections with environment variables

The environment-variables secrets backend is first in every search path, so environment variables are the simplest fixture:

| Kind | Variable name | Value |
| :--- | :--- | :--- |
| Variable `export_target` | `AIRFLOW_VAR_EXPORT_TARGET` | The raw value; JSON if you read it with `deserialize_json=True` |
| Connection `warehouse` | `AIRFLOW_CONN_WAREHOUSE` | A URI (`postgresql://user:pass@192.0.2.10:5432/db`) or a JSON object (`{"conn_type": "postgres", "host": "192.0.2.10", "login": "user", "password": "pass", "port": 5432, "schema": "db"}`) |

The name after the prefix is upper-cased (`AIRFLOW_CONN_{conn_id.upper()}` in `supervisor.py`). Values from these backends are never written to the DB and do not show in the UI.

```python
from airflow.sdk import Connection, Variable


def test_reads_config(monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_EXPORT_TARGET", "s3://example-bucket/out/")
    monkeypatch.setenv("AIRFLOW_CONN_WAREHOUSE", "postgresql://user:pass@192.0.2.10:5432/db")
    assert Variable.get("export_target") == "s3://example-bucket/out/"
    assert Connection.get("warehouse").host == "192.0.2.10"
```

For `dag.test()`, pass files instead: `conn_file_path` and `variable_file_path` point the run at `airflow.secrets.local_filesystem.LocalFilesystemBackend` (JSON or YAML files, `{"warehouse": "postgresql://..."}` and `{"export_target": "s3://..."}`).

---

## 5. `dag.test()` and `airflow dags test`

`dag.test()` runs one Dag run in-process, without a scheduler or executor: it creates the run, loops `update_state()` and runs each ready task in the same process, including triggers for deferred tasks.

```python
# at the bottom of a Dag file, or in a test
if __name__ == "__main__":
    orders_etl_dag = orders_etl()
    orders_etl_dag.test(
        run_conf={"full": True},
        conn_file_path="tests/fixtures/connections.json",
        variable_file_path="tests/fixtures/variables.json",
        mark_success_pattern=r"wait_for_.*|notify",
    )
```

| Argument | Meaning |
| :--- | :--- |
| `logical_date` | Defaults to now. Pass `None` explicitly for a run without a logical date |
| `run_after` | Defaults to now |
| `run_conf` | The run's `conf`; overrides params |
| `conn_file_path`, `variable_file_path` | Local secrets files for this run only |
| `mark_success_pattern` | Regex (`fullmatch`) of task ids to mark `success` instead of running: skip sensors and notifications |
| `use_executor` | Send tasks to the configured executor instead of running them in-process |

What it needs (from the `DAG.test` source):

* **A migrated metadata DB.** It raises `RuntimeError: Session not configured` without one. In CI: `airflow db migrate` against SQLite in a throwaway `AIRFLOW_HOME`.
* **The Dag file inside a configured bundle.** It syncs bundles to the DB before the run: the bundle that already owns the Dag, or, the first time, every configured bundle in turn until the Dag is found (which can include the `example_dags` bundle unless `[core] load_examples` is `False`) so that Dag versions and sibling Dags exist. Keep tests pointed at the real Dags folder (`[core] dags_folder`). Behavior for a Dag file outside every bundle is *(not verified against source)*.
* It clears earlier task instances for the same logical date first, and returns the `DagRun`.
* Human-in-the-loop tasks park the loop until a response arrives from outside.

The CLI wraps the same method and **exits non-zero ("DagRun failed")** when the run fails, so it works as a CI gate:

```bash
airflow dags test orders_etl 2026-01-01 -c '{"full": true}' --mark-success-pattern 'wait_for_.*'
airflow dags test orders_etl -f dags/orders_etl.py          # parse just this file
airflow dags test orders_etl --save-dagrun run.dot           # graph of the result (needs graphviz)
```

---

## 6. `airflow tasks test` and `airflow tasks render`

```bash
airflow dags reserialize                                     # put the current Dags in the DB first
airflow tasks test orders_etl extract 2026-01-01
airflow tasks test orders_etl process 2026-01-01 --map-index 3 --task-params '{"full": true}'
airflow tasks test orders_etl extract --env-vars '{"AIRFLOW_VAR_EXPORT_TARGET": "s3://example-bucket/tmp/"}'
airflow tasks render orders_etl extract 2026-01-01           # print rendered templated fields
```

Traps, from `task_test` in `airflow-core/src/airflow/cli/commands/task_command.py`:

* It loads the task's **serialized Dag from the database** (`get_db_dag`), so a Dag that the Dag processor or `airflow dags reserialize` has not stored fails with `Dag '<id>' could not be found in the database.`
* It ignores dependencies and creates a temporary Dag run if none exists for that date, deleting it afterwards. Upstream XComs are only there if a real run exists for that date.
* It does **not** exit non-zero when the task fails. Use `airflow dags test` or pytest for CI gating. Add `-m` (`--post-mortem`) to drop into a debugger on failure.

---

## 7. Linting for Airflow 3

Ruff ships Airflow rules (prefix `AIR`). The 3.4 docs recommend ruff 0.17.3 or later:

```bash
ruff check dags/ --select AIR                 # all Airflow rules
ruff check dags/ --select AIR301,AIR302 --fix # apply safe fixes for removed and moved names
```

| Rule group | Flags |
| :--- | :--- |
| AIR301, AIR302 | Names and arguments removed in Airflow 3 or moved to providers: breaking |
| AIR311, AIR312 | Names that still work but should move (for example to `airflow.sdk` or a provider): not yet breaking |
| AIR304 | Runtime-varying values in Dag and task constructors (the same problem as `[dag_processor] dag_version_inflation_check_level`) |

Rule codes are taken from the 3.4 upgrade guide and FAQ; check `ruff rule <code>` for your ruff version *(not verified against source)*. Pair the linter with `AIRFLOW__DAG_PROCESSOR__DAG_VERSION_INFLATION_CHECK_LEVEL=error` in staging so the Dag processor rejects what the linter missed.

---

## 8. CI recipe

```bash
#!/usr/bin/env bash
set -euo pipefail

export AIRFLOW_HOME="$(mktemp -d)"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DAG_PROCESSOR__DAG_VERSION_INFLATION_CHECK_LEVEL=error

ruff check dags/ --select AIR                      # 1. lint
pytest tests/ -q                                   # 2. integrity + unit tests (no DB needed)
airflow db migrate                                 # 3. throwaway SQLite DB in $AIRFLOW_HOME
airflow dags list-import-errors --local            # 4. what the Dag processor would report
airflow dags test orders_etl 2026-01-01 --mark-success-pattern 'wait_for_.*'   # 5. smoke runs
```

Notes:

* Install the same Airflow version, providers and constraints as production (see [installation](../04_infrastructure_and_cloud/01_installation_and_python.md)). A test that passes against another version proves little.
* `--local` makes `list-import-errors` parse the files instead of reading the DB.
* Keep smoke-run Dags free of external side effects with `mark_success_pattern`, environment-variable connections pointing at test systems, and `params` that select a dry-run mode.
* The guide's own [CLI reference](../09_reference/02_cli_reference.md#airflow-dags) lists every flag of `airflow dags`.

---

## 9. Checklist before merging a Dag change

| Check | Covered by |
| :--- | :--- |
| No import errors, no cycles | §2 |
| `dag_id` and task ids unchanged, or the rename is deliberate | Code review ([anti-patterns §4](../01_architecture_and_mental_model/02_anti_patterns.md#4-renaming-a-dag_id-or-task_id)) |
| No `Variable.get` / heavy imports at module level | Review, `airflow dags report` parse time |
| Every task has `retries` and `execution_timeout` | §2 convention test |
| Tasks are idempotent | Run `dag.test()` twice for the same date and compare outputs |
| Schedule does what you think | `airflow dags next-execution <dag_id>` on staging ([scheduling](02_scheduling_timetables_and_assets.md)) |
| Lint clean | §7 |

For running Airflow itself from a source checkout, see [building and testing from source](../08_developing_airflow/01_building_and_testing_from_source.md).
