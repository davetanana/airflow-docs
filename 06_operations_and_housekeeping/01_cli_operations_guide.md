# CLI Operations Guide: The Day-2 `airflow` Commands by Task

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

The official CLI reference lists every command alphabetically. This page groups the commands an operator actually uses by **task**: is it alive, why is this Dag missing, rerun these tasks, move pools and variables between environments, clean the database. It also says which commands touch the metadata database directly and which need the Dag files, because in Airflow 3 that decides *where* you can run them. Every command and flag here was checked against `airflow-core/src/airflow/cli/cli_config.py` on the `v3-4-stable` branch. The full flag list is in the [CLI reference](../09_reference/02_cli_reference.md).

---

## 1. Where to Run the CLI: Database, Dag Files, or the API

The `airflow` CLI in 3.4 is a **direct metadata-database client**. Even the commands that look remote (`dags trigger`, `dags delete`) call a "local client" that writes to the database through SQLAlchemy (`airflow/api/client/local_client.py`). It does not go through the REST API.

| Command needs | Examples | Run it from |
| :--- | :--- | :--- |
| Metadata DB only | `jobs check`, `dags list`, `dags list-runs`, `dags trigger`, `dags pause`, `tasks clear`, `tasks states-for-dag-run`, `pools *`, `variables *`, `connections *`, `db *`, `backfill create` | A scheduler, Dag processor or API server container (they already have `[database] sql_alchemy_conn`) |
| Dag files parsed locally (runs your code) | `dags report`, `dags list --local`, `dags list-import-errors --local`, `dags test`, `tasks test`, `dags reserialize` | A container that has the Dag bundle and your Python dependencies, normally the Dag processor |
| REST API with a user token | `airflowctl ...` | Anywhere that can reach the API server. No DB credentials needed |

> [!IMPORTANT]
> **Never call the `airflow` CLI from inside a task.** In Airflow 3, workers talk to the API server through the Execution API and are not supposed to hold metadata-DB credentials. A `BashOperator` running `airflow db clean` either fails or forces you to hand DB credentials to every worker. Use a CronJob or an external scheduler instead (see [Housekeeping](02_housekeeping_and_automations.md#3-automating-db-clean-safely)).

### `airflowctl`: the API-based CLI

`airflowctl` is a separate distribution (`airflow-ctl/` in the source tree) that talks to the public REST API with a JWT, so it obeys the auth manager's permissions. In 3.4 many `airflow` commands carry a source-only `@deprecated_for_airflowctl(...)` marker naming their `airflowctl` equivalent (`airflowctl dags trigger`, `airflowctl jobs check`, `airflowctl pools import`, ...). The marker prints **no warning** at runtime (`airflow/cli/utils.py`); it tells you where new development goes. The `db`, `dags report`, `dags test`, `rotate-fernet-key` and server commands have no `airflowctl` replacement.

```bash
airflowctl auth login --api-url https://airflow.example.internal --username <user>   # prompts for the password
airflowctl jobs check --job-type SchedulerJob --allow-multiple --limit 10
```

See [REST API and airflowctl](../09_reference/05_rest_api_and_airflowctl.md).

---

## 2. Health: Is Each Component Alive?

`airflow jobs check` reads the `job` table and asks whether recent jobs of a type are `running` with a fresh heartbeat. It is what the Helm chart uses as the liveness probe for the scheduler, triggerer and Dag processor (`chart/templates/_helpers.yaml`).

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--job-type` | any | `SchedulerJob`, `TriggererJob` or `DagProcessorJob` |
| `--hostname` | none | Only jobs whose hostname matches |
| `--local` | off | Only jobs whose hostname equals this host's `[core] hostname_callable` result |
| `--limit` | `1` | How many of the most recent running jobs to check; `0` disables the limit |
| `--allow-multiple` | off | Succeed even if several alive jobs match |

```bash
# Liveness probe for the scheduler in this pod
airflow jobs check --job-type SchedulerJob --local

# Is at least one scheduler alive anywhere (HA with several schedulers)?
airflow jobs check --job-type SchedulerJob --allow-multiple --limit 100

# Same for triggerers and Dag processors
airflow jobs check --job-type TriggererJob --allow-multiple --limit 100
airflow jobs check --job-type DagProcessorJob --allow-multiple --limit 100
```

Outputs and exit codes (from `airflow/cli/commands/jobs_command.py`):

| Output | Exit | Meaning |
| :--- | :--- | :--- |
| `Found one alive job.` | 0 | Healthy |
| `Found N alive jobs.` | 0 | Healthy, only with `--allow-multiple` |
| `No alive jobs found.` | 1 | Nothing heartbeating within the threshold |
| `Found N alive jobs. Expected only one.` | 1 | You checked an HA deployment without `--allow-multiple` |
| `To use option --allow-multiple, you must set the limit to a value greater than 1 or 0 to disable it.` | 1 | `--allow-multiple` with the default `--limit 1` |

> [!WARNING]
> `--allow-multiple` with the default `--limit 1` is an error, not a no-op. Always pair it with `--limit`.

"Alive" means `state = running` and a heartbeat newer than `[scheduler] scheduler_health_check_threshold` (30 s), `[triggerer] triggerer_health_check_threshold` (30 s) or `[dag_processor] health_check_threshold` (30 s) for the matching job type (`airflow/jobs/job.py`). HTTP health endpoints are covered in [Monitoring §4](03_monitoring_and_observability.md#4-health-endpoints).

Other quick checks:

```bash
airflow db check --retry 3 --retry-delay 5    # can this container reach the metadata DB?
airflow db check-migrations -t 60             # wait up to 60 s for the schema to match this Airflow version
airflow version
airflow info --anonymize                      # versions, paths, providers; safe to paste in a ticket
airflow providers list
airflow plugins
```

---

## 3. Inspecting Dags

| Question | Command |
| :--- | :--- |
| Which Dags does the DB know, and are they paused? | `airflow dags list` (default columns `dag_id, fileloc, owners, is_paused, bundle_name, bundle_version`) |
| Only one bundle | `airflow dags list -B <bundle_name>` |
| Which files fail to import? | `airflow dags list-import-errors` |
| Same, but parse here instead of reading the DB | `airflow dags list-import-errors --local` |
| How long does each file take to parse? | `airflow dags report -o table` |
| Dag attributes as stored | `airflow dags details <dag_id> -o yaml` |
| Recent runs | `airflow dags list-runs <dag_id> --state failed --limit 20` |
| Runs in a window, excluding backfills | `airflow dags list-runs <dag_id> -s 2026-11-01 -e 2026-11-07 --no-backfill` |
| State of one run | `airflow dags state <dag_id> <logical_date_or_run_id>` |
| When will it run next? | `airflow dags next-execution <dag_id> -n 5` |
| Task list | `airflow tasks list <dag_id>` |
| Why won't this task get scheduled? | `airflow tasks failed-deps <dag_id> <task_id> <run_id>` |
| What will the templates render to? | `airflow tasks render <dag_id> <task_id> <run_id>` |

`dags report` instantiates a `BundleDagBag` for each bundle, so it **executes your top-level Dag code** on the machine where you run it. Use it in the Dag processor container to find slow files: anything close to `[core] dagbag_import_timeout` (30 s) is about to start timing out.

`--output` (`-o`) accepts `table`, `json`, `yaml` and `plain` on every listing command. Use `-o json` with `jq` in scripts.

---

## 4. Runs and Tasks: Trigger, Pause, Clear

### Trigger and pause

```bash
airflow dags trigger <dag_id>                                    # new manual run now
airflow dags trigger <dag_id> -r manual__fix_2026_11_23 -c '{"full_refresh": true}'
airflow dags trigger <dag_id> -l 2026-11-22T00:00:00+00:00       # explicit logical date

airflow dags pause <dag_id>
airflow dags unpause <dag_id>
airflow dags pause '^etl_.*' --treat-dag-id-as-regex -y          # pause a family of Dags
```

> [!NOTE]
> Triggering a **paused** Dag creates the run, but it stays `queued` and no task runs until you unpause (this is the command's own help text). New Dags start paused because `[core] dags_are_paused_at_creation` defaults to `True`.

### Clear (rerun) task instances

`airflow tasks clear` resets matching task instances so the scheduler runs them again. The `dag_id` is positional.

| Flag | Meaning |
| :--- | :--- |
| `-t`, `--task-regex` | Only task_ids matching this regex |
| `-s`, `--start-date` / `-e`, `--end-date` | Window of runs to clear |
| `-f`, `--only-failed` | Only failed task instances |
| `-r`, `--only-running` | Only running task instances |
| `-u`, `--upstream` / `-d`, `--downstream` | Include upstream or downstream tasks of the matches |
| `-R`, `--dag-regex` | Treat `dag_id` as a regex |
| `-B`, `--bundle-name` | Restrict to a bundle |
| `-y`, `--yes` | Do not prompt |

```bash
# Rerun every failed task (and what depends on it) from last week
airflow tasks clear daily_sales -s 2026-11-16 -e 2026-11-22 -f -d

# Rerun one task across many Dags
airflow tasks clear '^ingest_' -R -t '^load_to_warehouse$' -s 2026-11-22 -e 2026-11-22 -y
```

> [!WARNING]
> Without `-s`/`-e`, `tasks clear` matches **all** runs. Without `-y` it shows the list and asks first; read the list. Clearing a running task makes its worker receive a 409 on the next heartbeat and kill the task (see [Troubleshooting §5](05_troubleshooting_playbook.md#5-task-killed-server-indicated-the-task-shouldnt-be-running-anymore)).

For **partitioned** Dags (runs keyed by `partition_key` / `partition_date`), use `airflow dags clear <dag_id>` with exactly one of `--run-id`, `--partition-key`, or `--partition-date-start` / `--partition-date-end`, plus optional `-f` / `-r`. The command's help sends non-partitioned Dags back to `tasks clear`.

### Task state

```bash
airflow tasks states-for-dag-run <dag_id> <run_id> -o table
airflow tasks state <dag_id> <task_id> <run_id> --map-index 3     # one mapped instance
```

`tasks state` and `states-for-dag-run` accept a logical date or a `run_id` in the same positional slot. Prefer `run_id`: asset-triggered and manual runs can have no logical date.

---

## 5. Backfills

In Airflow 3 a backfill is a database object that the **scheduler** executes; the CLI only creates it and returns. Closing the terminal does not stop it. Pause or cancel it from the UI (Dag > Backfills) or the REST API.

```bash
# Preview first: prints the runs it would create
airflow backfill create --dag-id daily_sales \
  --from-date 2026-10-01 --to-date 2026-10-31 --dry-run

# Create it: rerun missing and failed runs, at most 3 at a time
airflow backfill create --dag-id daily_sales \
  --from-date 2026-10-01 --to-date 2026-10-31 \
  --reprocess-behavior failed --max-active-runs 3
```

| Flag | Meaning |
| :--- | :--- |
| `--from-date` / `--to-date` | Inclusive window; required. For partitioned Dags it is a partition-date range |
| `--reprocess-behavior` | `none` (missing runs only, the default), `failed` (missing and failed), `completed` (all) |
| `--max-active-runs` | Concurrency for this backfill |
| `--run-backwards` | Newest first; refused if any task uses `depends_on_past` |
| `--dag-run-conf` | JSON conf for every created run |
| `--run-on-latest-version` / `--no-run-on-latest-version` | Experimental: which bundle version to run |
| `--dry-run` | Print, create nothing |

Backfill runs count against the backfill's `--max-active-runs`, not the Dag's `max_active_runs`. The scheduler logs `dag cannot be started due to backfill max_active_runs constraint; ...` when it is holding runs back.

---

## 6. Pools, Variables and Connections: Export and Import

Use these to promote settings between environments or to snapshot them before a risky change.

```bash
# Pools: JSON file in, JSON file out
airflow pools export pools.json
airflow pools import pools.json
airflow pools set etl_db 8 "Warehouse connections" --include-deferred

# Variables: export to a file, or "-" for stdout
airflow variables export variables.json
airflow variables import variables.json -a skip        # overwrite (default) | fail | skip
airflow variables list --show-values --hide-sensitive

# Connections: json, yaml or env
airflow connections export connections.yaml --file-format yaml
airflow connections import connections.yaml --overwrite
airflow connections test <conn_id>
```

> [!WARNING]
> `variables export` and `connections export` write values **decrypted, in plaintext**, including passwords and extras. Treat the files as secrets: write them to an encrypted volume, never to a shared bucket or a ticket, and delete them after the import.

Notes:
* `variables import` overwrites existing keys by default. Use `-a fail` in CI to catch drift, `-a skip` to only add missing keys.
* `connections import` skips existing connection ids unless you pass `--overwrite`.
* Connections and variables served by a secrets backend (`[secrets] backend`) are not in the metadata DB, so these commands neither export nor overwrite them.
* `connections test` is refused unless `[core] test_connection` is changed from its default `Disabled`.

---

## 7. Configuration Inspection

```bash
airflow config get-value scheduler task_queued_timeout
airflow config list --section scheduler --show-values --include-sources
airflow config list --section logging --show-values --hide-sensitive
airflow config list --defaults > airflow.cfg.defaults           # every option, commented out
```

> [!TIP]
> `config list` hides values unless you pass `--show-values` and prints `Values are hidden by default. Use `--show-values` to display them (may include sensitive data).` to stderr. `--include-sources` (`-s`) tells you whether a value came from `airflow.cfg`, an environment variable, a `_cmd`/`_secret` option or the default, which is the fastest way to find "why is this setting not taking effect?".

> [!WARNING]
> `config get-value` prints **nothing and exits 0** for a section or key that does not exist (it catches `AirflowConfigException`). A typo looks like an empty value. Check the name in the [airflow.cfg reference](../09_reference/01_airflow_cfg_reference.md).

`airflow config lint` and `airflow config update` report and rewrite options that changed between Airflow 2.x and 3.0; they are upgrade tools, covered in [Upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md).

The CLI reads configuration in the container where it runs. If the scheduler and API server get different environment variables, `config list` in one tells you nothing about the other: run it in each.

---

## 8. Database Commands

| Command | Use | Risk |
| :--- | :--- | :--- |
| `airflow db check` | Connectivity test (`--retry`, `--retry-delay`) | None |
| `airflow db check-migrations -t 60` | Wait until the schema matches this version; use in init containers | None |
| `airflow db migrate` | Upgrade the schema (`--show-sql-only`, `-n/--to-version`, `-r/--to-revision`) | Back up first |
| `airflow db downgrade -n <version>` | Downgrade schema before rolling back binaries | **Irreversible data loss possible** |
| `airflow db clean --clean-before-timestamp <ts>` | Purge old rows | **Irreversible**; see [Housekeeping](02_housekeeping_and_automations.md) |
| `airflow db export-archived --output-path <dir>` | Dump `_airflow_deleted__*` archive tables to CSV | None, unless `--drop-archives` |
| `airflow db drop-archived` | Drop archive tables | Irreversible |
| `airflow db shell` | Open `psql`/`mysql` with the configured URL | Whatever you type |
| `airflow db reset` | Drop and recreate **everything** | **Destroys all history**. Never on production |
| `airflow state-store clean --dry-run` | Delete expired task-state rows (metastore backend only) | Irreversible without `--dry-run` |

> [!IMPORTANT]
> There is no `airflow db init` or `airflow db upgrade` in Airflow 3. `airflow db migrate` creates the schema on an empty database and upgrades an existing one. <!-- check_docs: ignore -->

---

## 9. Everything Else Worth Knowing

* `airflow cheat-sheet` prints every command grouped by area. Useful on a jump host with no browser.
* `airflow dags reserialize -B <bundle>` re-parses and rewrites serialized Dags, for example after an upgrade changed the serialization format.
* `airflow dags delete <dag_id> -y` deletes the Dag's DB records (runs, task instances, versions). If the file is still in a bundle, the Dag processor re-creates the Dag on its next parse; delete the file first.
* `airflow rotate-fernet-key` re-encrypts connections, variables, triggers and connection-test requests with the first key in `[core] fernet_key`. See [Housekeeping §7](02_housekeeping_and_automations.md#7-rotating-the-fernet-key).
* `airflow scheduler` responds to `SIGUSR2` by dumping the executor's tracked task state to its log: `pkill -f -USR2 "airflow scheduler"`.
* `airflow teams ...` exists for multi-team deployments (`[core] multi_team`).

---

**Next:** automate the cleanup commands in [Housekeeping and Automations](02_housekeeping_and_automations.md), and look up any flag in the [CLI reference](../09_reference/02_cli_reference.md).
