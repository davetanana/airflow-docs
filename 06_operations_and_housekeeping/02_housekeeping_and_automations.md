# Housekeeping and Automations: Keeping the Metadata DB and Logs Small

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

Airflow never deletes history on its own. Every Dag run, task try, XCom, audit-log row and Dag version stays in the metadata database until you remove it, and every task attempt leaves a log file behind. This page covers what grows, how `airflow db clean` decides what to delete (from `airflow-core/src/airflow/utils/db_cleanup.py` on `v3-4-stable`), how to schedule it safely now that tasks can no longer reach the database, and the other recurring chores: log retention, stale Dags and bundle versions, expired task state and Fernet key rotation. It ends with alert thresholds that tell you a chore was missed.

---

## 1. What Grows, and Why It Hurts

| Table | Grows with | Why it hurts when large |
| :--- | :--- | :--- |
| `task_instance` | Every task try (3.4 keeps earlier tries in the same table as archived rows) | Scheduler queries and UI grid views slow down |
| `dag_run` | Every run, including backfills | Scheduler `max_active_runs` checks, UI |
| `xcom_v2` (and legacy `xcom_v1`) | Every pushed return value | Disk; large values bloat backups |
| `log` | The audit log: every UI/API/CLI action and task state change | Often the largest table by row count |
| `job` | Every scheduler/triggerer/Dag processor process start | Small but unbounded |
| `asset_event` | Every asset update | Asset views and asset-triggered scheduling |
| `dag_version` | Every structural change to a Dag | See [§6](#6-stale-dags-dag-versions-and-bundles) |
| `import_error`, `trigger`, `callback`, `deadline`, `revoked_token`, `task_state_store` | Errors, deferrals, callbacks, deadline alerts, logouts, task state | Usually small; clean them anyway |

Check sizes on PostgreSQL before deciding on a retention period:

```sql
SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid)) AS total
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 15;
```

---

## 2. `airflow db clean`: What It Actually Does

```bash
airflow db clean --clean-before-timestamp '2026-08-25 00:00:00+00:00' --dry-run
```

`--clean-before-timestamp` is **required**. A timestamp without timezone is read in `[core] default_timezone`.

### Tables and the column that decides "old"

| `--tables` value | Recency column | Special rules |
| :--- | :--- | :--- |
| `dag_run` | `start_date` | Keeps the latest **non-manual** run per Dag even if it is old, so scheduling continuity is preserved. Pulls in `task_instance`, `task_state_store`, `deadline` |
| `task_instance` | `start_date` | Pulls in `xcom_v1`, `xcom_v2`, `task_reschedule` |
| `task_instance_history` | `start_date` | Accepted as a name: cleans only archived earlier tries (rows not in the live working set) |
| `xcom` | `timestamp` | Alias that expands to `xcom_v1` and `xcom_v2` |
| `log` | `dttm` | Audit log |
| `job` | `latest_heartbeat` | |
| `dag` | `last_parsed_time` | Removes Dags not parsed since the cutoff, with their `dag_version` and `deadline` rows |
| `dag_version` | `created_at` | Keeps the latest version per Dag; skips versions still referenced by any task instance |
| `asset_event` | `timestamp` | `--asset-ids 1,2` narrows it, only with `--tables asset_event` |
| `partitioned_asset_key_log` | `created_at` | Only orphan rows whose partition Dag run is gone |
| `callback` | `created_at` | Only terminal or unknown states, and scheduled callbacks no deadline references |
| `trigger` | `created_date` | |
| `deadline` | `deadline_time` | |
| `import_error` | `timestamp` | |
| `task_reschedule`, `task_state_store`, `revoked_token`, `connection_test_request`, `celery_taskmeta`, `celery_tasksetmeta`, `_xcom_archive` | various | `connection_test_request`: only `success`/`failed` |
| `session` | `expiry` | Only listed when the FAB auth manager uses database sessions |

Without `--tables`, every table above is cleaned. Unknown names are skipped with a warning (`The following table(s) are not valid choices and will be skipped: ...`), not an error, so check the spelling in the dry-run output.

### Flags

| Flag | Default | Effect |
| :--- | :--- | :--- |
| `--dry-run` | off | Prints the config per table and the first 100 matching rows; deletes nothing |
| `-t`, `--tables` | all | Comma-separated list |
| `--dag-ids` / `--exclude-dag-ids` | none | Only, or never, these Dags (applied through foreign keys on tables without a `dag_id`) |
| `--batch-size N` | unbounded | Delete or archive at most N rows per transaction; each batch gets its own archive table |
| `--skip-archive` | off | Delete without copying rows to `_airflow_deleted__*` tables |
| `-y`, `--yes` | off | Skip the confirmation prompt |
| `--error-on-cleanup-failure` | off | Exit non-zero if any table failed |
| `-v`, `--verbose` | off | More output |

> [!WARNING]
> **Without `--error-on-cleanup-failure`, a failed table is a warning and the command exits 0.** The log says `Encountered error when attempting to clean table '<name>'.` and finally `The following tables were not cleaned due to errors: [...]`. A cron job that only checks the exit code reports success while nothing was deleted. Always pass the flag in automation.

> [!IMPORTANT]
> Without `--yes`, the command prints `This is irreversible.  Consider backing up the tables first and / or doing a dry run with option --dry-run.` and requires you to type exactly `delete rows`. Anything else exits with `User did not confirm; exiting.`

### Archive tables

Unless you pass `--skip-archive`, deleted rows are first copied to a new table named `_airflow_deleted__<table>__<YYYYMMDDHHMMSS>` (plus `__b<N>` per batch with `--batch-size`). They are **in the same database**, so a first clean does not free space; it moves it. Export and drop them:

```bash
mkdir -p /backup/airflow-archive
airflow db export-archived --output-path /backup/airflow-archive          # CSV per table; directory must exist
airflow db export-archived --output-path /backup/airflow-archive --drop-archives -y
airflow db drop-archived -t dag_run,task_instance -y                      # drop without export
```

`drop-archived` and `export-archived --drop-archives` ask you to type `drop archived tables` unless you pass `-y`.

PostgreSQL does not return freed pages to the OS after a large delete. Autovacuum makes the space reusable; run `VACUUM (ANALYZE)` on the big tables after the first large clean, and plan a maintenance window if you really need `VACUUM FULL`.

---

## 3. Automating `db clean` Safely

A "maintenance Dag" that runs `airflow db clean` in a `BashOperator` was a common Airflow 2 pattern. In Airflow 3 it is the wrong tool: workers are not supposed to have metadata-DB credentials (they use the Execution API), so the task either fails or forces you to widen DB access to every worker. Run the CLI **outside** the task path, from something that already has DB access.

### Option A: the Helm chart's `databaseCleanup` CronJob

The chart in the 3.4 source tree (chart version 2.0.0, versioned independently of Airflow) ships a disabled-by-default CronJob that runs `airflow db clean --clean-before-timestamp <now - retentionDays> --yes`:

```yaml
databaseCleanup:
  enabled: true
  schedule: "0 3 * * 0"       # chart default is "0 0 * * 0" (Sunday midnight)
  retentionDays: 90           # chart default
  skipArchive: true           # otherwise archive tables pile up in the same DB
  batchSize: 10000
  tables: []                  # empty = all tables
  verbose: true
```

> [!NOTE]
> The chart's command does not pass `--error-on-cleanup-failure`. Override `databaseCleanup.args` to add it if your alerting relies on the job's exit status.

### Option B: your own CronJob or cron entry

```bash
#!/usr/bin/env bash
# Run where `airflow` is installed with the same config as the scheduler.
set -euo pipefail
CUTOFF=$(date -u -d '-90 days' +%Y-%m-%dT%H:%M:%S+00:00)
airflow db check --retry 3 --retry-delay 10
airflow db clean --clean-before-timestamp "$CUTOFF" \
  --skip-archive --batch-size 10000 --error-on-cleanup-failure --yes
airflow state-store clean
```

Rules of thumb:
* **Take a backup first**, at least the first time. There is no undo. See [Backup and disaster recovery](../07_disaster_recovery/01_backup_and_disaster_recovery.md).
* Run the first clean manually with `--dry-run`, then table by table (`-t log`, then `-t xcom`, then `-t dag_run`) on a large backlog. Deleting `dag_run` cascades into task instances and XComs, the slowest part.
* Use `--batch-size` on a busy database. Without it, each table is one transaction that holds row locks the scheduler may need.
* Pick retention per table if you need audit history: run `-t log` with a longer cutoff, then the rest with a shorter one using `--tables` lists.
* Schedule it off-peak, and never two at once.

---

## 4. Task Log Retention

Each task attempt writes a file under `[logging] base_log_folder` (default `{AIRFLOW_HOME}/logs`) on the worker, named by `[logging] log_filename_template` (`dag_id=.../run_id=.../task_id=.../attempt=N.log`). Schedulers, triggerers and Dag processors also log to that tree (`[logging] dag_processor_child_process_log_directory` defaults to `{AIRFLOW_HOME}/logs/dag_processor`). Nothing in Airflow core deletes these files.

| Where | Retention mechanism |
| :--- | :--- |
| Helm chart pods | `logGroomerSidecar` on Celery workers, scheduler, triggerer and Dag processor: `retentionDays: 15`, `retentionMinutes: 0`, `frequencyMinutes: 15`, `maxSizeBytes: 0` (disabled) by default |
| VMs / your own containers | A cron `find` over `base_log_folder`, or `logrotate` |
| Remote storage (S3, GCS, Azure Blob) | A bucket lifecycle rule. Airflow never deletes remote logs |
| Worker disk after upload | `[logging] delete_local_logs = True` deletes the local copy after a successful upload to GCS, S3, WASB, HDFS or OSS |

The groomer runs `/clean-logs` from the Airflow image. In the main-branch copy of that script (`scripts/docker/clean-logs.sh`), it deletes `*.log` files older than the retention under `$AIRFLOW_HOME/logs`, then removes empty directories; with a size limit set it lowers the retention one day at a time until the volume fits *(the script ships with the image, not the 3.4 source tree; check the one in your image)*.

For a VM:

```bash
# Daily: delete task and component log files older than 15 days, then empty directories
find "$AIRFLOW_HOME/logs" -type f -name '*.log' -mtime +15 -delete
find "$AIRFLOW_HOME/logs" -type d -empty -delete
```

> [!WARNING]
> If you delete local logs without remote logging, the UI shows a "log not found" message for older tries. Pick log retention and DB retention together: there is no point keeping 90 days of task instances whose logs are gone after 15. See [Logging](04_logging.md).

---

## 5. Expired Task State

Airflow 3.4 adds a task and asset state store (`[state_store] backend`, default `airflow.state.metastore.MetastoreBackend`). Rows get an `expires_at` from `[state_store] default_retention_days` (30). No running component deletes expired rows: in the 3.4 source the backend's `cleanup()` is called only by the CLI command below. Schedule it with your DB cleanup:

```bash
airflow state-store clean --dry-run
airflow state-store clean
```

It only handles the default metastore backend; custom backends are skipped. Batch size comes from `[state_store] state_cleanup_batch_size` (`0` = no batching). `airflow db clean -t task_state_store` also cleans the table by `expires_at`.

---

## 6. Stale Dags, Dag Versions and Bundles

**Deleted Dag files.** When a file disappears from a bundle, the Dag processor marks its Dags inactive (`deactivate_deleted_dags` in `dag_processing/manager.py`). Dags that are still in a re-parsed file but no longer defined are deactivated after `[dag_processor] stale_dag_threshold` (50 s). Inactive Dags keep their history. To remove them completely:

```bash
airflow dags delete <dag_id> -y                  # all DB records for one Dag
airflow db clean -t dag --clean-before-timestamp '2026-08-25 00:00:00+00:00' --dry-run
```

**Dag version accumulation.** Every structural change to a Dag creates a new `dag_version` row and serialized Dag. A Dag whose structure changes on every parse (a `datetime.now()` in a `start_date`, a random task id, a generated description) creates a version every `[dag_processor] min_file_process_interval` (30 s). Warnings:
* `[dag_processor] dag_version_inflation_check_level` (`warning` by default; `error` makes it an import error; `off` disables it) checks Dag constructors for runtime-varying values before parsing.
* The counter `dag.serialization.version_created` (tagged with `dag_id` and `bundle_name`) should be near zero outside deployments. Alert on a steady rate.

`airflow db clean -t dag_version` keeps the latest version per Dag and skips any version still referenced by a task instance, so clean task instances first or in the same run.

**Bundle checkouts.** Versioned bundles (Git) keep old checkouts on local disk under `[dag_processor] dag_bundle_storage_path`. They are pruned every `[dag_processor] stale_bundle_cleanup_interval` (1800 s) once older than `[dag_processor] stale_bundle_cleanup_age_threshold` (21600 s), always keeping `[dag_processor] stale_bundle_cleanup_min_versions` (10). Size the volume for that.

**Rendered fields.** Rendered template fields are kept for the last `[core] num_dag_runs_to_retain_rendered_fields` (30) runs per task; lower it if templated fields are large.

---

## 7. Rotating the Fernet Key

`[core] fernet_key` encrypts connection passwords and extras, variables, trigger kwargs and connection-test requests. It accepts a comma-separated list: the **first** key encrypts, all keys decrypt (`MultiFernet` in `models/crypto.py`).

1. Generate a new key (any Fernet key generator; for example `cryptography.fernet.Fernet.generate_key()`), and store it in your secrets manager.
2. Set `AIRFLOW__CORE__FERNET_KEY=<new_key>,<old_key>` on **every** component: API server, scheduler, Dag processor, triggerer and workers. Restart them.
3. Run `airflow rotate-fernet-key` once, from a container with DB access. It re-encrypts all rows in one transaction, in batches of 100.
4. Set `AIRFLOW__CORE__FERNET_KEY=<new_key>` everywhere and restart.

> [!WARNING]
> If you remove the old key before step 3 finished, or a component missed step 2, encrypted values can no longer be decrypted and connections fail at runtime. Back up the database before rotating, and keep the old key until you have checked a few connections with `airflow connections get <conn_id>`.

`rotate-fernet-key` does not touch secrets held in an external `[secrets] backend`.

---

## 8. Alerts That Catch a Missed Chore

| Signal | Suggested threshold (illustrative) | Means |
| :--- | :--- | :--- |
| Metadata DB size | > 80% of the volume, or growth > 5% per week | `db clean` not running or retention too long |
| `log` table rows | Steady growth with no weekly drop | Cleanup job failing silently (missing `--error-on-cleanup-failure`) |
| Count of `_airflow_deleted__%` tables | > 0 for more than a week | Archives never exported or dropped |
| Log volume usage on workers/scheduler | > 80% | Groomer disabled or retention too long |
| `dag.serialization.version_created` | Sustained non-zero rate outside deploys | Dag version inflation |
| `dag_processing.import_errors` | > 0 | Broken Dag files (see [Troubleshooting](05_troubleshooting_playbook.md)) |
| CronJob last successful run | Older than 2× its schedule | Job is failing or suspended |

```sql
-- PostgreSQL: leftover archive tables
SELECT tablename FROM pg_tables WHERE tablename LIKE '\_airflow\_deleted\_\_%';
```

---

**See also:** the commands themselves in the [CLI operations guide](01_cli_operations_guide.md), DB sizing in [Metadata database](../04_infrastructure_and_cloud/04_metadata_database.md), and metrics in [Monitoring and observability](03_monitoring_and_observability.md).
