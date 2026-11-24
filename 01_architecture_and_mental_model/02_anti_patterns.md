# Anti-Patterns and Traps: What Hurts Airflow 3.4 in Practice

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Best Practices](https://airflow.apache.org/docs/apache-airflow/3.4.0/best-practices.html) · [FAQ](https://airflow.apache.org/docs/apache-airflow/3.4.0/faq.html)

The official "best practices" page lists recommendations. This page lists the mistakes that actually cause slow parsing, stuck schedulers, surprise reruns and lost history in Airflow 3.4, each with **why it hurts**, **how to spot it**, and **the fix**. Config keys and defaults are taken from `config.yml` on `v3-4-stable`; behavior claims cite the source file. Read the [mental model](01_mental_model.md) first if the terms "Dag processor", "Dag version" or "Execution API" are new.

---

## 1. Heavy top-level code and imports in Dag files

* **The anti-pattern**: work at module level of a Dag file: `import pandas, tensorflow`, reading a large config file, calling an API to decide which tasks to create.
* **Why it hurts**:
  * The Dag processor re-imports every file every `[dag_processor] min_file_process_interval` (30 s by default) using `[dag_processor] parsing_processes` (2) subprocesses. Module-level work runs on every parse, forever, not once per run.
  * One file's import is bounded by `[core] dagbag_import_timeout` (30 s; `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT`). Past that the file gets an import error and its Dags disappear from the schedule.
  * The whole parsing subprocess is killed after `[dag_processor] dag_file_processor_timeout` (50 s), counted in the `dag_processing.processor_timeouts` metric.
  * Slow files delay every other file in the queue, so a Dag change takes longer to show up.
* **How to spot it**:
  ```bash
  airflow dags report                 # parse duration, Dag and task count per file
  airflow dags list-import-errors     # timeouts show up here
  ```
  Metrics: `dag_processing.last_duration` per file and `dag_processing.total_parse_time` (see [monitoring](../06_operations_and_housekeeping/03_monitoring_and_observability.md)).
* **The fix**: move heavy imports and I/O inside the task function. Keep the module to Dag structure only.
  ```python
  from airflow.sdk import dag, task


  @dag(schedule=None)
  def train():
      @task
      def fit(path: str) -> str:
          import pandas as pd  # imported only when the task runs

          df = pd.read_parquet(path)
          return str(len(df))

      fit("s3://example-bucket/features.parquet")


  train()
  ```
  If one file generates many Dags, use `get_parsing_context()` from `airflow.sdk` to build only the Dag a worker asks for (its `dag_id` is set when a worker parses the file to run a task).

---

## 2. Variable and Connection access at parse time

* **The anti-pattern**: `Variable.get("tables")` or `Connection.get("warehouse")` at module level, or in a Dag or operator argument that is evaluated during parsing.
* **Why it hurts**:
  * Each call is a round trip from the parsing subprocess to the Dag processor manager and on to the configured secrets backends (`airflow-core/src/airflow/dag_processing/processor.py` handles `GetVariable` / `GetConnection`), on every parse of the file. With a cloud secrets backend this is a paid API call every 30 seconds per file.
  * `[secrets] use_cache` is `False` by default, so nothing is cached unless you turn it on (`[secrets] cache_ttl_seconds`, 900 s, then applies).
  * A missing Variable raises `AirflowRuntimeError` (`VARIABLE_NOT_FOUND`) and the whole file becomes an import error.
  * A value that changes between parses changes the Dag structure, creating new Dag versions (§3).
* **How to spot it**: `grep -n "Variable.get\|Connection.get" dags/*.py` outside `@task` bodies; secrets-backend API bills; import errors that mention `Variable`.
* **The fix**: read values at run time, through templates or inside tasks:
  ```python
  from airflow.providers.standard.operators.bash import BashOperator

  BashOperator(task_id="export", bash_command="export.sh {{ var.value.export_target }}")
  ```
  If you really need a parse-time value, pass `default=` to `Variable.get()` so a missing key does not break the file, and enable `[secrets] use_cache`.

---

## 3. Non-deterministic Dag definitions

* **The anti-pattern**: `start_date=datetime.now()`, `start_date=pendulum.today()`, a task argument built from `random` or `uuid4()`, a `doc_md` with a timestamp.
* **Why it hurts**:
  * The serialized Dag hash differs on every parse. When the current Dag version has task instances, every changed hash creates a new `dag_version` row (`SerializedDagModel.write_dag`), so the UI shows hundreds of versions and the `dag_version` and `serialized_dag` tables grow.
  * A moving `start_date` also moves the first schedulable tick, so runs can be skipped or never created.
* **How to spot it**: the Dag processor's static checker (3.2+) flags calls such as `datetime.now()`, `date.today()`, `time.time()`, `random.randint()`, `uuid.uuid4()` and `pendulum.now()` inside Dag and task constructors and shows a Dag warning in the UI. `[dag_processor] dag_version_inflation_check_level` controls it: `warning` (default), `off`, or `error`, which turns the finding into an import error. Ruff's AIR304 rule catches the same thing in CI (see [testing Dags](../02_dag_authoring/03_testing_dags.md)).
* **The fix**: use a fixed, timezone-aware start date.
  ```python
  import pendulum

  START = pendulum.datetime(2026, 1, 1, tz="UTC")
  ```
  Set `[dag_processor] dag_version_inflation_check_level = error` in CI and staging so these never reach production.

---

## 4. Renaming a `dag_id` or `task_id`

* **The anti-pattern**: "cleaning up" names on a Dag that already has history.
* **Why it hurts**:
  * A new `dag_id` is a new Dag. It starts paused if `[core] dags_are_paused_at_creation` is `True` (the default), with no run history. Depending on its schedule and `catchup`, it may run immediately. The old Dag is deactivated once its file has been re-parsed without it (after `[dag_processor] stale_dag_threshold`, 50 s) but its runs stay under the old id.
  * A renamed task looks like "task removed, new task added". In existing runs the old task instances go to `removed` and the new task gets fresh task instances, which may run inside old Dag runs when they are cleared. `depends_on_past` on the new task has no past to look at.
* **How to spot it**: diffs that touch `dag_id=`, `task_id=`, or a `@task` function name (the function name is the default task id).
* **The fix**: treat ids as primary keys. Change `dag_display_name` (Dag) or add a `Label` instead of renaming. If you must rename, do it between runs, unpause the new Dag deliberately, and delete the old Dag's metadata only after you no longer need its history.

---

## 5. Large XComs

* **The anti-pattern**: returning a DataFrame, a file's contents or a long list from a task.
* **Why it hurts**: XComs are stored in the `xcom` table as JSON (`JSONB` on PostgreSQL), and in 3.x every push and pull goes through the API server's Execution API. Core sets no size limit (there is no XCom size key in `config.yml`), so the database and the API server absorb the whole payload, and the UI may try to render it.
* **How to spot it**: a growing `xcom` table; slow task start or end; API server memory spikes.
* **The fix**: write data to object storage and pass a reference. For automatic offload, set `[core] xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend` with the Common IO provider's `xcom_objectstorage_path` and `xcom_objectstorage_threshold` (default `-1`, which keeps everything in the database). Provider versions are independent of core; check the provider's docs for its keys. See [XCom, Variables and Connections](../03_task_execution/04_xcom_variables_and_connections.md).

---

## 6. Poke-mode sensors holding slots

* **The anti-pattern**: sensors with the default `mode="poke"` waiting hours for a file or partition.
* **Why it hurts**: a poking sensor is a running task. It holds a worker slot, a pool slot (`default_pool` has `[core] default_pool_task_slot_count` = 128 slots) and a place in `max_active_tasks` for the whole wait. `BaseSensorOperator` defaults are `mode="poke"`, `poke_interval=60`, and `timeout` from `[sensors] default_timeout` (604800 s, seven days).
* **How to spot it**: many `running` sensor tasks with almost no CPU; pools at capacity while workers are idle.
* **The fix**, in order of preference:
  1. **Deferrable** operators or sensors (`deferrable=True` where supported). The task moves to `deferred` and the triggerer waits with no worker slot. `[operators] default_deferrable` (default `false`) turns it on for operators that read it. See [deferrable operators](../03_task_execution/02_deferrable_operators_and_triggers.md).
  2. `mode="reschedule"`: the task releases its slot between pokes and goes to `up_for_reschedule`.
  3. Always set an explicit `timeout` shorter than seven days.

---

## 7. Unbounded dynamic task mapping

* **The anti-pattern**: `process.expand(item=list_files())` where `list_files` can return tens of thousands of entries.
* **Why it hurts**: each element becomes a task instance row the scheduler must evaluate. `[core] max_map_length` (default 1024) is the guard: when a task's return value has more elements, the **API server** rejects the push (`unmappable_return_value_length` in `execution_api/routes/xcoms.py`) and the upstream task fails. Because it is enforced on the API server, raising the key on workers alone does nothing.
* **The fix**: batch upstream (return 200 chunks of 50 items, not 10,000 items), cap fan-out with `max_active_tis_per_dagrun`, and raise `[core] max_map_length` only on purpose, on the API server.

---

## 8. Catchup and backlog surprises

* **The anti-pattern**: setting `catchup=True` (or `[scheduler] catchup_by_default = True`) with an old `start_date`; or unpausing an asset-triggered Dag with a long backlog of events.
* **Why it hurts**:
  * Catchup creates a run for every missed tick since `start_date`, up to `max_active_runs` (16) at a time, competing with real work. `catchup=True` without a `start_date` fails validation (`start_date is required when catchup=True`).
  * Since 3.4, asset-triggered Dags create **one run per asset event** by default (`[scheduler] batch_asset_events = False`). `catchup=False` does not drop a queued backlog for asset-triggered Dags, so 500 queued events become 500 runs.
* **The fix**: keep `catchup=False` and use `airflow backfill create` with `--max-active-runs` for history, after a `--dry-run`. For asset consumers that only need the latest state, pass `batch_asset_events=True` to the timetable. See [scheduling](../02_dag_authoring/02_scheduling_timetables_and_assets.md).

---

## 9. Non-idempotent tasks

* **The anti-pattern**: `INSERT` without a key, appending to "today's" file, using `now()` to decide which data to process.
* **Why it hurts**: Airflow will run a task more than once: retries, clears, backfills, a worker lost after the work finished but before the state was recorded (heartbeat timeout, §5 of the [mental model](01_mental_model.md#5-life-of-a-task-instance)). Each rerun duplicates data.
* **The fix**: make each task a function of its run: write to a partition keyed by `data_interval_start` or `dag_run.run_id`, use `MERGE`/upsert or delete-then-insert in one transaction, and derive dates from the context, not the wall clock. Remember `logical_date` can be `None` on manual and asset-triggered runs. Patterns in [Dag design patterns](../02_dag_authoring/01_dag_design_patterns.md#2-idempotent-tasks).

---

## 10. Direct metadata database access from tasks

* **The anti-pattern**: `from airflow.settings import Session`, `@provide_session`, or a `PostgresHook` pointed at the Airflow metadata DB from inside a task, to read other Dags' state or bulk-edit Variables.
* **Why it hurts**: Airflow 3 blocks it. The task supervisor removes `engine` and `Session` from `airflow.settings`, replaces `configure_orm` with a function that raises `RuntimeError: Database access is disabled from Dags and Triggers`, and points `[database] sql_alchemy_conn` at `airflow-db-not-allowed:///` (`supervisor.py`, `block_orm_access`). Bypassing it with a hand-built connection string works technically but breaks the security model and couples your Dags to an internal schema that changes every release.
* **The fix**: use the SDK (`Variable`, `Connection`, XCom, `context["ti"]`), the REST API, or `airflowctl`. For "wait for another Dag", use assets or `ExternalTaskSensor` from the standard provider.

---

## 11. One giant Dag (or one giant file)

* **The anti-pattern**: a single Dag with thousands of tasks spanning teams, or one Python file that generates hundreds of Dags.
* **Why it hurts**: the whole file is re-parsed when any part changes; one import error takes every Dag in the file down; the serialized Dag and every version of it are large; the UI grid is slow; all tasks share one `max_active_tasks` (default `[core] max_active_tasks_per_dag` = 16) and one failure policy.
* **The fix**: split by ownership and schedule. Connect Dags with assets instead of one dependency graph. Use task groups for visual structure within a Dag.

---

## 12. `depends_on_past` chains

* **The anti-pattern**: `default_args={"depends_on_past": True}` on every task "to be safe".
* **Why it hurts**: one failed or never-run task instance blocks that task in every later run until someone clears or marks it. The scheduler keeps re-evaluating the blocked task instances. `airflow backfill create --run-backwards` is not supported when tasks depend on past (per the CLI help).
* **The fix**: set it only on the task that truly needs ordered processing. `[scheduler] ignore_first_depends_on_past_by_default` (default `True`) already lets a new task start without history. Alert on the first failure instead of letting the chain silently stall.

---

## 13. Misreading asset schedules

* **The anti-pattern**: `schedule=[asset_a, asset_b]` expecting "run when either updates".
* **Why it hurts**: a list is converted to `AssetAll(...)` (`DAG._default_timetable` in `task-sdk/src/airflow/sdk/definitions/dag.py`): the Dag runs only when **both** have new events. AND conditions also force event batching.
* **The fix**: write the condition explicitly: `schedule=(asset_a | asset_b)` for "either", `schedule=(asset_a & asset_b)` for "both".

---

## 14. Quick reference: guards and defaults

| Trap | Guard or setting | 3.4 default |
| :--- | :--- | :--- |
| Slow file import | `[core] dagbag_import_timeout` | 30 s |
| Stuck parsing process | `[dag_processor] dag_file_processor_timeout` | 50 s |
| Re-parse frequency | `[dag_processor] min_file_process_interval` | 30 s |
| Runtime-varying Dag values | `[dag_processor] dag_version_inflation_check_level` | `warning` |
| Parse-time secrets cost | `[secrets] use_cache` / `cache_ttl_seconds` | `False` / 900 s |
| Fan-out | `[core] max_map_length` (enforced by API server) | 1024 |
| Sensor wait | `[sensors] default_timeout` | 604800 s |
| Catchup | `[scheduler] catchup_by_default` | `False` |
| Asset event per run | `[scheduler] batch_asset_events` | `False` |
| Concurrency per Dag | `[core] max_active_tasks_per_dag` / `max_active_runs_per_dag` | 16 / 16 |
| Auto-pause failing Dags | `[core] max_consecutive_failed_dag_runs_per_dag` | 0 (off) |
| New Dags paused | `[core] dags_are_paused_at_creation` | `True` |

Next: [Dag design patterns](../02_dag_authoring/01_dag_design_patterns.md) shows what to write instead.
