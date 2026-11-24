# Tutorial: An Operations Drill on a Local Deployment

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

This page is a hands-on drill for whoever runs Airflow: check that every component is alive, break a Dag file and find the error, make a task fail and retry, recover the run two different ways, backfill a date range, and preview a metadata cleanup. Every command and flag was checked against `airflow-core/src/airflow/cli/cli_config.py` on the 3.4 branch. The official docs describe each command in isolation; this page puts them in the order you'll need them during an incident, with the traps that bite.

Prerequisites: `airflow standalone` running from the [quickstart](01_quickstart.md), a second terminal with the same `AIRFLOW_HOME` exported, and `curl` and `jq`.

---

## 1. Check component health

Each long-running component writes a heartbeat row to the `job` table. `airflow jobs check` reads it:

```bash
airflow jobs check --job-type SchedulerJob --local
airflow jobs check --job-type DagProcessorJob --local
airflow jobs check --job-type TriggererJob --local
```

Each prints `Found one alive job.` and exits 0, or exits non-zero with `No alive jobs found.`. Flags: `--job-type` (one of `SchedulerJob`, `TriggererJob`, `DagProcessorJob`), `--local` (only jobs whose hostname matches this machine) or `--hostname <name>` (the two can't be combined), `--limit` (default `1`, `0` for no limit), and `--allow-multiple` (succeed when several are alive, for HA schedulers). The official Docker Compose file uses exactly these commands as container health checks.

The API server exposes the same information over HTTP, without authentication:

```bash
curl -s http://localhost:8080/api/v2/monitor/health | jq
```

The response has `metadatabase`, `scheduler`, `triggerer` and `dag_processor` objects, each with `status` (`healthy` or `unhealthy`) and a `latest_*_heartbeat`. The 3.4 response also adds `detailed_status` (`healthy`, `degraded`, `down`) and an `instances` list per component. A component counts as unhealthy when its last heartbeat is older than its threshold: `[scheduler] scheduler_health_check_threshold`, `[triggerer] triggerer_health_check_threshold` and `[dag_processor] health_check_threshold`, each `30` seconds by default.

> [!WARNING]
> The 2.x path `/health` no longer serves health data. In 3.4 it returns an error body pointing to `/api/v2/monitor/health`. Update load-balancer and Kubernetes probes when you upgrade.

Two more checks worth scripting:

```bash
airflow db check                                   # can this host reach the metadata database?
airflow config get-value core executor             # which executor is actually configured?
```

The scheduler can also serve its own HTTP health endpoint on `[scheduler] scheduler_health_check_server_port` (default `8974`), but only when `[scheduler] enable_health_check` is `True`; it defaults to `False`. See [monitoring](../06_operations_and_housekeeping/03_monitoring_and_observability.md).

---

## 2. Deploy the drill Dag

Save this as `$AIRFLOW_HOME/dags/ops_drill.py`. Whether `flaky` fails is controlled by an Airflow Variable, so you can break and fix it without editing code:

```python
from datetime import timedelta

import pendulum

from airflow.sdk import Param, Variable, dag, task


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["ops"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=15)},
    params={"note": Param("", type="string")},
)
def ops_drill():
    @task
    def flaky(*, ti, params):
        if Variable.get("ops_drill_fail", default="false") == "true":
            raise RuntimeError(f"Failing on purpose, try {ti.try_number}")
        print(f"flaky succeeded, note={params['note']!r}")

    @task
    def downstream():
        print("downstream ran")

    flaky() >> downstream()


ops_drill()
```

```bash
airflow variables set ops_drill_fail false
airflow dags list-import-errors
airflow dags list | grep ops_drill
```

If `ops_drill` is missing, wait for the Dags-folder refresh (seconds under standalone, up to `[dag_processor] refresh_interval` = `300` seconds otherwise) or run `airflow dags reserialize`.

---

## 3. Find an import error

Create a broken file on purpose:

```bash
printf 'from airflow.sdk import dag\n\n@dag(\ndef broken():\n    pass\n' > "$AIRFLOW_HOME/dags/broken_drill.py"
```

After the next parse, list the errors recorded by the Dag processor:

```bash
airflow dags list-import-errors
```

You get the file path and the `SyntaxError` with its line. The same list is the red banner on the UI's Dags page. Two things to know:

* `list-import-errors` reads what the **Dag processor** stored in the database. To parse the files yourself, on this host, ignoring the database, add `-l/--local`. Use that to tell "the file is broken" apart from "the Dag processor isn't running" or "this host has a different Python environment than the Dag processor".
* An import error hides the Dag only if it fails before the Dag is defined. A Dag that was already parsed stays in the UI with its last good version, so "nothing changed after my deploy" often means "the new version doesn't import". Check this list before anything else. *(Keeping the last good version is not verified against source.)*

Remove the file:

```bash
rm "$AIRFLOW_HOME/dags/broken_drill.py"
```

The import error disappears on the next parse of the bundle. See the [troubleshooting playbook](../06_operations_and_housekeeping/05_troubleshooting_playbook.md) for import timeouts and Dags that vanish.

---

## 4. Pause and unpause

```bash
airflow dags unpause ops_drill
airflow dags pause ops_drill
airflow dags unpause ops_drill
```

What pausing does and does not do:

| While a Dag is paused | Behavior |
| :--- | :--- |
| Scheduled runs | Not created. With `catchup=False`, unpausing creates only the latest due run, not the ones you missed. |
| Runs you trigger | Created, but they stay `queued`. The `dags trigger` help text says so. |
| Backfill runs | Also stay `queued`: the scheduler only starts queued runs of unpaused Dags (`airflow-core/src/airflow/models/dagrun.py`). |
| Tasks already running | Keep running to completion. Pausing is not a kill switch. |

Both commands accept `--treat-dag-id-as-regex` to act on several Dags; they then ask for confirmation unless you pass `-y/--yes`. New Dags start paused because `[core] dags_are_paused_at_creation` defaults to `True`.

---

## 5. Trigger with conf and a logical date, and watch it fail and retry

Turn the failure on, then trigger a run with a fixed run ID, a logical date, and conf:

```bash
airflow variables set ops_drill_fail true
airflow dags trigger ops_drill -r drill_1 -l 2026-11-20T00:00:00+00:00 -c '{"note": "first drill"}'
```

Flags: `-r/--run-id`, `-l/--logical-date`, `-c/--conf` (JSON, validated against the Dag's `params`; an invalid conf is rejected before the run is created).

> [!IMPORTANT]
> Pass `-l` when you will later need to select this run **by date**. Without it, 3.x creates the run with no logical date. Such runs can't be matched by `airflow tasks clear -s` / `-e`, which filter on the logical date, and their tasks have no `ds` or `ts`. The run ID always works. Also, `(dag_id, logical_date)` is unique: a second trigger with the same `-l` fails.

Watch the states, repeating every few seconds:

```bash
airflow tasks states-for-dag-run ops_drill drill_1
```

You'll see `flaky` go `running` → `up_for_retry` → `running` …, three attempts in total (`retries=2`), 15 seconds apart. Then `flaky` is `failed`, `downstream` is `upstream_failed`, and the run is `failed`:

```bash
airflow dags state ops_drill drill_1
ls "$AIRFLOW_HOME/logs/dag_id=ops_drill/run_id=drill_1/task_id=flaky/"
```

There is one log file per attempt: `attempt=1.log`, `attempt=2.log`, `attempt=3.log`. When a task fails "for no reason", read the **first** attempt's log, because later attempts often fail differently (for example on data the first attempt half-wrote).

---

## 6. Recovery option A: fix the cause and clear

Fix the cause, then clear only the failed task instances of that logical date so the scheduler runs them again:

```bash
airflow variables set ops_drill_fail false
airflow tasks clear ops_drill -s 2026-11-20T00:00:00+00:00 -e 2026-11-20T00:00:00+00:00 -f
```

The command lists the task instances it is about to clear and asks for confirmation; answer `y`. `-f/--only-failed` selects `failed` **and** `upstream_failed`, so `downstream` is included. The run goes back to `queued`, then succeeds.

| Flag | Meaning |
| :--- | :--- |
| `-t/--task-regex <regex>` | Only matching task IDs. Combine with `-u/--upstream` or `-d/--downstream` to pull in relatives. |
| `-s/--start-date`, `-e/--end-date` | Bounds on the run's **logical date**, inclusive. |
| `-f/--only-failed`, `-r/--only-running` | Restrict by state. |
| `-y/--yes` | Skip the confirmation. Use with care in scripts. |

> [!WARNING]
> Without `-s` and `-e`, `airflow tasks clear` matches **every** run of the Dag, so `airflow tasks clear ops_drill -y` re-runs its entire history. Always read the list it prints before you confirm. To clear runs that have no logical date, clear them by run in the UI, or through the REST API (`POST /api/v2/dags/{dag_id}/clearTaskInstances`).

`airflow dags clear` also exists in 3.4, but it selects runs by `--run-id`, `--partition-key` or a partition-date window, and is meant for partitioned Dags.

---

## 7. Recovery option B: mark success

Sometimes the work was done by hand, or the failed task doesn't matter for this run. Make another run fail:

```bash
airflow variables set ops_drill_fail true
airflow dags trigger ops_drill -r drill_2
```

Wait until `airflow dags state ops_drill drill_2` prints `failed`. The 3.4 `airflow tasks` group has no mark-success command; use the UI (task instance → **Mark as → Success**) or the REST API. With the Simple auth manager, get a token first:

```bash
PASSWORD=$(jq -r '.admin' "$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated")
TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H 'Content-Type: application/json' \
  -d "{\"username\": \"admin\", \"password\": \"${PASSWORD}\"}" | jq -r .access_token)

curl -s -X PATCH http://localhost:8080/api/v2/dags/ops_drill/dagRuns/drill_2/taskInstances/flaky \
  -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' \
  -d '{"new_state": "success", "note": "done by hand during ops drill"}' | jq '.task_instances[].state'
```

Setting a task's state also clears its downstream tasks that are `failed` or `upstream_failed` (`set_task_instance_state` in `airflow-core/src/airflow/serialization/definitions/dag.py`), so `downstream` runs and the run completes. The `note` is shown in the UI, which is where the next on-call engineer will look. The body also accepts `include_upstream`, `include_downstream`, `include_future` and `include_past`. See [REST API & airflowctl](../09_reference/05_rest_api_and_airflowctl.md).

Turn the failure off again:

```bash
airflow variables set ops_drill_fail false
```

---

## 8. Backfill a date range

In 3.x, a backfill is not a long-running CLI process. `airflow backfill create` writes a backfill record and its Dag runs to the database, and the **scheduler** runs them. Preview first:

```bash
airflow backfill create --dag-id ops_drill --from-date 2026-11-15 --to-date 2026-11-17 --dry-run
```

The dry run prints its parameters and then "Runs to be attempted": logical dates 15, 16 and 17 November (both ends inclusive, one run per daily trigger). Then create it for real, limited to one run at a time:

```bash
airflow backfill create --dag-id ops_drill --from-date 2026-11-15 --to-date 2026-11-17 --max-active-runs 1
airflow dags list-runs ops_drill
```

Rules enforced in `airflow-core/src/airflow/models/backfill.py`:

| Rule | Error you'll see |
| :--- | :--- |
| One active backfill per Dag | `Another backfill is running for Dag ops_drill.` |
| No dates in the future | `Backfill cannot be executed for future dates.` |
| The schedule must be periodic: not `None`, `@once`, `@continuous` or asset-triggered | `... has a non-periodic schedule that does not support backfills` |
| `--run-backwards` is refused when any task has `depends_on_past=True` | `Backfill cannot be run in reverse ...` |

`--reprocess-behavior` decides what happens to dates that already have a run: `none` (default, only missing dates), `failed` (missing and failed), `completed` (all). Pass `--dag-run-conf '<json>'` to give the runs a conf. Remember that backfill runs stay queued while the Dag is paused (§4).

---

## 9. Preview a metadata cleanup

Every run, task instance, log row and XCom stays in the metadata database until you delete it. `airflow db clean` is the supported way. Always run it with `--dry-run` first:

```bash
airflow db clean --clean-before-timestamp '2026-11-18 00:00:00+00:00' --dry-run
```

`--clean-before-timestamp` is required; rows older than it are candidates. The dry run prints, per table, how many rows would be deleted, and changes nothing. Restrict with `-t/--tables` (comma-separated, for example `-t dag_run,task_instance,xcom_v2,log`) or `--dag-ids` / `--exclude-dag-ids`. For `dag_run`, the latest non-manual run of each Dag is always kept, even when older than the timestamp, so the scheduler still knows where the schedule stands (`keep_last` in `airflow-core/src/airflow/utils/db_cleanup.py`).

> [!CAUTION]
> Without `--dry-run`, deletion is real. By default, deleted rows are copied to archive tables named `_airflow_deleted__<table>__<timestamp>`, so the database doesn't shrink until you export and drop them (`airflow db export-archived`, `airflow db drop-archived`). `--skip-archive` deletes without a copy. `-y/--yes` skips the confirmation prompt. Take a backup before the first real run; see [housekeeping](../06_operations_and_housekeeping/02_housekeeping_and_automations.md) and [backup & DR](../07_disaster_recovery/01_backup_and_disaster_recovery.md).

---

## 10. Clean up the drill

```bash
airflow dags pause ops_drill
rm "$AIRFLOW_HOME/dags/ops_drill.py"
airflow dags delete ops_drill -y
airflow variables delete ops_drill_fail
```

`airflow dags delete` removes the Dag's runs, task instances and history from the database; it does not delete the file. Delete the file first, or the Dag processor re-creates the Dag on the next parse.

---

## 11. Where to go next

| You want to… | Read |
| :--- | :--- |
| Every CLI command for day-to-day work, by task | [CLI operations guide](../06_operations_and_housekeeping/01_cli_operations_guide.md), [CLI reference](../09_reference/02_cli_reference.md#airflow-tasks) |
| Symptom-to-fix runbooks | [Troubleshooting playbook](../06_operations_and_housekeeping/05_troubleshooting_playbook.md) |
| Metrics and alerting on the signals used here | [Monitoring & observability](../06_operations_and_housekeeping/03_monitoring_and_observability.md) |
| Schedule `db clean` and other housekeeping | [Housekeeping & automations](../06_operations_and_housekeeping/02_housekeeping_and_automations.md) |
| Do the same against a remote deployment | [REST API & airflowctl](../09_reference/05_rest_api_and_airflowctl.md) |
| Run a real deployment | [Deployment runbook](../05_deployment_and_config/02_deployment_runbook.md) |
