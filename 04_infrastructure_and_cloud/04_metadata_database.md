# The Metadata Database

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

The metadata database holds every Dag run, task try, serialized Dag, connection, variable and audit event. When it is slow, the whole deployment is slow; when it is lost, so is the deployment's history and most of its configuration. The official "Set up a Database Backend" how-to covers creating the database and the URL format. This page adds what you need to run it: which component opens how many connections, PgBouncer, the 3.4 driver change, how migrations behave and fail, and which tables grow without bound in 3.4 (several of them were renamed or merged in 3.4).

Sources: `airflow-core/src/airflow/settings.py` (engine and pool), `configuration.py` (URL rewriting), `utils/db.py` (migrations), `utils/db_cleanup.py`, the migration files in `airflow-core/src/airflow/migrations/versions/`, and `[database]` in `config.yml`.

---

## 1. Supported Backends

| Backend | Versions tested for 3.4 | Use |
| :--- | :--- | :--- |
| PostgreSQL | 14, 15, 16, 17, 18 | Recommended for production |
| MySQL | 8.0, 8.4, Innovation releases | Supported. Validated in CI only with the `mysqlclient` driver (`mysql+mysqldb://`) |
| SQLite | 3.15.0 or newer (checked at startup) | Development and tests only. Default: `sqlite:///{AIRFLOW_HOME}/airflow.db` |
| MariaDB | Not supported | Known index-handling differences; migrations are not tested on it |
| MS SQL Server | Not supported | Dropped in 2.x |

Notes:

* **SQLite is not for production.** It works with the default `LocalExecutor` on one machine, but it serializes writers and cannot support more than one scheduler.
* **Multiple schedulers** need row-level locking with `SKIP LOCKED` (`[scheduler] use_row_level_locking`, default `True`): Postgres, or MySQL 8.
* **MySQL** needs a UTF-8 database (`CHARACTER SET utf8mb4`), `explicit_defaults_for_timestamp=1`, and Airflow forces the `READ COMMITTED` isolation level on MySQL connections (`prepare_engine_args` in `settings.py`). ID columns get `utf8mb3_bin` collation (`[database] sql_engine_collation_for_ids`) to keep indexes within size limits.
* **Managed Postgres** (RDS, Cloud SQL, Azure) closes idle connections. Keep `[database] sql_alchemy_pool_pre_ping = True` (the default) and consider TCP keepalives through `[database] sql_alchemy_connect_args`.

---

## 2. Who Talks to the Database in Airflow 3

Airflow 3 moved task code off the database. Knowing which processes connect tells you where to put credentials and how to size pools.

| Process | Connects directly? | Notes |
| :--- | :--- | :--- |
| Scheduler | Yes, heavily | The main source of load and lock contention |
| API server | Yes | UI, REST API and the Execution API that workers call |
| Dag processor | Yes (the manager) | Writes serialized Dags, `dag_version`, `dag_code`, import errors. Parsing subprocesses are steered through the Execution API |
| Triggerer | Yes (the job) | Claims and updates `trigger` rows; trigger code is steered through the Execution API |
| Celery worker | Only for the Celery result backend, if it uses the database | Task code uses the Execution API with a per-task JWT |
| KubernetesExecutor task pod | No | Task code uses the Execution API |
| `airflow db` CLI commands, migration Job | Yes | Run from a trusted admin context |

Tasks that used to query the metadata DB directly (`settings.Session`, `@provide_session` in task code) do not work in Airflow 3. Use the REST API or the Task SDK instead. See [Mental model](../01_architecture_and_mental_model/01_mental_model.md).

---

## 3. Connection URL and Driver

Set the URL with `[database] sql_alchemy_conn` (`AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`), or keep it out of the environment with `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD` / `_SECRET`.

```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg://airflow:<password>@10.0.12.5:5432/airflow"
airflow db check --retry 5 --retry-delay 2
```

**3.4 change: psycopg 3 is the default Postgres driver.** `configuration.py` (`_upgrade_postgres_metastore_conn`) rewrites the scheme at startup:

| You write | Airflow uses |
| :--- | :--- |
| `postgresql+psycopg://` | As written |
| `postgresql://` | `postgresql+psycopg://` if `psycopg` is importable, otherwise `postgresql+psycopg2://` |
| `postgres://`, `postgres+psycopg2://` | Same rewrite, plus a `FutureWarning` that SQLAlchemy no longer accepts the scheme |
| `postgresql+psycopg2://` | As written; requires `psycopg2` (`apache-airflow-providers-postgres[psycopg2]`) |

The async engine (used by the API server) comes from `[database] sql_alchemy_conn_async`, or is derived from the sync URL with `psycopg_async` when unset.

**Prepared statements.** For psycopg 3 URLs Airflow sets `prepare_threshold=None` on both engines (`_psycopg3_connect_args` in `settings.py`), which turns off server-side prepared statements. This keeps it safe behind PgBouncer in transaction mode. Anything you set in `[database] sql_alchemy_connect_args` is merged on top and wins, so don't put `prepare_threshold` back unless your pooler is PgBouncer 1.22+ with `max_prepared_statements > 0` and the client uses libpq 17+.

> [!WARNING]
> `[database] sql_alchemy_connect_args` must resolve to a mapping (an import path to a `dict`). If it does not, Airflow logs that the psycopg3 default was not applied and engine creation is likely to fail.

---

## 4. Connection Pools and Connection Math

Each Airflow process that touches the database creates a SQLAlchemy `QueuePool` (unless `[database] sql_alchemy_pool_enabled = False`, which switches to `NullPool`). Processes that also use the async engine get a **second pool with the same limits**.

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `[database] sql_alchemy_pool_enabled` | `True` | `False` = open and close a connection per checkout (use with an external pooler if you want) |
| `[database] sql_alchemy_pool_size` | `5` | Connections kept open per pool |
| `[database] sql_alchemy_max_overflow` | `10` | Extra connections allowed under load, closed when returned |
| `[database] sql_alchemy_pool_recycle` | `1800` | Seconds before a connection is replaced. Keep it below the server's or proxy's idle timeout |
| `[database] sql_alchemy_pool_pre_ping` | `True` | Test each connection on checkout |
| `[database] sql_alchemy_engine_args` | unset | JSON of extra engine arguments. When set, it **replaces** the built-in Postgres defaults (`insertmanyvalues_page_size` etc.); pool keys above are still applied on top |
| `[database] max_db_retries` | `3` | Retries for some operational errors (deadlocks, lost connections) |

Worst case per pool is `pool_size + max_overflow` = 15 connections. A rough upper bound for direct connections (illustrative; real usage is usually well below the overflow ceiling):

```text
max_connections_needed ≈ Σ over processes (pools_per_process × (pool_size + max_overflow))

Example (illustrative): 2 schedulers + 2 API servers × [api] workers=1 (sync + async pool)
                        + 1 Dag processor + 2 triggerers, default pool settings:
  schedulers      2 × 1 × 15 = 30
  API servers     2 × 2 × 15 = 60
  Dag processor   1 × 1 × 15 = 15
  triggerers      2 × 1 × 15 = 30
  total                        135   plus Celery result backend, CLI jobs, KEDA, monitoring
```

Practical rules:

1. **Count processes, not pods.** `[api] workers` (default `1`) starts that many API server worker processes, each with its own pools. The setting's description recommends more API server replicas over more workers.
2. **Leave headroom** on the server (`max_connections` on Postgres) for migrations, `airflow db` CLI runs, backups and humans: at least 20%.
3. **Lower `max_overflow` before raising `max_connections`.** Overflow connections are opened and closed constantly, which is expensive on Postgres (a process per connection).
4. On Postgres, put **PgBouncer** in front once you have more than a handful of components (§5).

---

## 5. PgBouncer

PgBouncer in transaction mode multiplexes hundreds of client connections onto a few server connections. Recommended for every production Postgres deployment.

| Setting | Recommendation |
| :--- | :--- |
| `pool_mode` | `transaction` (what the Helm chart uses) |
| `max_client_conn` | Above the §4 total for everything that connects through it |
| `default_pool_size` / per-db `pool_size` | The real server connections; size to the database, not to the clients. Start at 20-50 for a medium deployment *(illustrative)* |
| `ignore_startup_parameters` | Include `extra_float_digits` |
| Prepared statements | Leave Airflow's psycopg 3 default (`prepare_threshold=None`) in place |

With PgBouncer you can keep Airflow's own pools (cheap connections to the local proxy) and let PgBouncer bound the server side. Point the migration Job at the database **directly** if your migrations hit PgBouncer limits or statement timeouts; long DDL through transaction pooling works but holds a server connection for the whole migration step.

The Helm chart's PgBouncer settings are in [Kubernetes and the Helm chart §5](03_kubernetes_and_helm_chart.md#5-external-database-and-pgbouncer).

---

## 6. Schema Migrations

| Command | What it does |
| :--- | :--- |
| `airflow db migrate` | Creates the schema on an empty database, or upgrades to the head revision of the installed code. Options: `-r/--to-revision`, `-n/--to-version`, `-s/--show-sql-only` (with `--from-revision`/`--from-version` to print offline SQL) |
| `airflow db check-migrations -t 60` | Waits up to `-t/--migration-wait-timeout` seconds (default `60`) for the DB to match the code; exits non-zero otherwise. What the Helm chart's init containers run |
| `airflow db check` | Only tests connectivity (`--retry`, `--retry-delay`) |
| `airflow db downgrade -n <version>` | Downgrades the schema; needs `-r/--to-revision` or `-n/--to-version`. Prompts unless `-y` |
| `airflow db reset` | **Drops all Airflow tables** and recreates them. Never on production |

The 2.x `init` and `upgrade` subcommands of `airflow db` no longer exist in Airflow 3.

**How components react to a schema mismatch.** Long-running commands (`scheduler`, `api-server`, `dag-processor`, `triggerer`) check the Alembic heads at startup when `[database] check_migrations = True` (the default). Without a TTY they log "Database migration required. Please run `airflow db migrate`" and exit 1; with a TTY they offer to run it. The same check fails when the database is **newer** than the code, for example after restoring a newer backup or rolling back the image without the schema.

**Revision map.** `_REVISION_HEADS_MAP` in `utils/db.py` maps versions to heads; for 3.4.0 the head is `b4f1a7d2c903`. The full list is in `airflow-core/docs/migrations-ref.rst`.

```bash
airflow db migrate --show-sql-only      # print the SQL without running it
```

**3.4.0 runs 26 migrations**, several of them heavy on big tables:

| Migration | Effect on large installs |
| :--- | :--- |
| `0142` Unify task attempt ownership | Renames `xcom` → `xcom_v1` and `rendered_task_instance_fields` → `rtif_v1`; copies **every row of `task_instance_history` into `task_instance`**, then drops `task_instance_history`; changes `task_instance` unique keys |
| `0143`/`0144` Dynamic region storage, rename stored map index | Rebuilds unique constraints on `task_instance` and `task_state_store` |
| `0136` Fold `task_map` into `xcom.mapped_length` | Rewrites mapped-task metadata |
| `0124`, `0126`, `0127`, `0129`, `0130` | New indexes on `asset_event`, `asset`, `serialized_dag`/`dag_code`, `trigger` |

Migration runtime grows with the size of `task_instance`, the old `task_instance_history` and `xcom`. Before upgrading a long-lived deployment, run `airflow db clean` (§7) on the old version, take a backup, and time the migration on a restored copy.

**How a failed migration leaves the database.** Alembic runs with `transaction_per_migration=True` (`migrations/env.py`):

* **Postgres**: each migration is one transaction. A failure rolls back that migration only; the database stays at the last successful revision, somewhere between the old and new version. Fix the cause and run `airflow db migrate` again; it resumes from there.
* **MySQL**: DDL commits implicitly, so a failing migration can leave a half-applied step. Restore from the pre-upgrade backup rather than trying to repair it by hand.

See [Backup and disaster recovery §6](../07_disaster_recovery/01_backup_and_disaster_recovery.md#6-recovering-from-a-failed-migration).

> [!IMPORTANT]
> Stop the scheduler, triggerer, Dag processor and API server before migrating. `airflow db migrate` takes a global migration lock against other migrations, not against running components.

---

## 7. Table Growth Hot Spots (3.4 Table Names)

| Table | Grows with | Notes |
| :--- | :--- | :--- |
| `task_instance` | Every task try | **In 3.4 it also holds past tries** (`working_set` is `NULL` for archived attempts, with `archived_reason`); `task_instance_history` no longer exists |
| `xcom_v2` / `xcom_v1` | Every pushed XCom | `xcom_v1` is the renamed pre-3.4 `xcom` table (values stored before 3.4, keyed by Dag/task/run/map index); `xcom_v2` holds new values keyed by the task-attempt UUID. Large values belong in object storage (custom XCom backend) |
| `rtif_v2` / `rtif_v1` | Rendered template fields per task try | Renamed `rendered_task_instance_fields` |
| `log` | Audit events: UI/API/CLI actions and task state events | Often the largest table by row count |
| `job` | Scheduler, triggerer and Dag processor job rows | Small rows, but many over years |
| `dag_run` | Every run | Cleaning it cascades to its task instances |
| `dag_version`, `serialized_dag`, `dag_code` | Every structural change of a Dag that already has task instances on its current version | A Dag whose structure changes on every parse (timestamps or random values in task IDs or arguments) creates a new version per run |
| `asset_event`, `asset_dag_run_queue` | Asset-driven scheduling | |
| `trigger`, `callback`, `deadline` | Deferrable tasks, callbacks, deadline alerts | |
| `celery_taskmeta`, `celery_tasksetmeta` | Celery result backend in the DB | |

Purge with `airflow db clean`, which archives rows into `_airflow_deleted__*` tables first unless you pass `--skip-archive`:

```bash
CLEAN_TS="$(date -u -d '-90 days' +%Y-%m-%dT%H:%M:%S)"    # GNU date
airflow db clean --clean-before-timestamp "${CLEAN_TS}" \
  --tables log,job,xcom_v2,task_instance,dag_run --batch-size 10000 --dry-run
# review, then run again without --dry-run, with --yes for non-interactive use
airflow db drop-archived --yes      # removes the archive tables once exported or not needed
```

`dag_version` rows still referenced by any task instance are skipped, and the newest version per Dag is always kept. Use `--error-on-cleanup-failure` in automation so a failed table makes the job fail. Schedules and batching are covered in [Housekeeping and automations](../06_operations_and_housekeeping/02_housekeeping_and_automations.md).

---

## 8. Health Checks and Quick Diagnosis

```bash
airflow db check                                   # can we connect at all?
airflow db check-migrations -t 1                   # schema matches this code?
airflow jobs check --job-type SchedulerJob --allow-multiple --limit 5
airflow db shell                                   # psql / mysql / sqlite3 with Airflow's credentials
```

```sql
-- Postgres: who holds connections, by application and state
SELECT application_name, state, count(*) FROM pg_stat_activity
WHERE datname = current_database() GROUP BY 1, 2 ORDER BY 3 DESC;

-- Postgres: biggest Airflow tables
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size, n_live_tup
FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 15;
```

| Symptom | Likely cause | Fix |
| :--- | :--- | :--- |
| `remaining connection slots are reserved` / `too many connections` | §4 sum exceeds `max_connections` | PgBouncer; lower `sql_alchemy_max_overflow`; fewer API server workers |
| `SSL SYSCALL error: EOF detected`, `server closed the connection unexpectedly` | Idle connections killed by a proxy or managed DB | Keep `sql_alchemy_pool_pre_ping`; lower `sql_alchemy_pool_recycle` below the idle timeout; TCP keepalives |
| `InvalidSqlStatementName` / `DuplicatePreparedStatement` | psycopg 3 prepared statements behind transaction pooling | Remove any `prepare_threshold` override from `sql_alchemy_connect_args` |
| Scheduler loop slow, high DB CPU | Bloated `task_instance`, `log`, `xcom_v2`; missing vacuum | `airflow db clean`; check autovacuum |
| Component exits "Database migration required" | Schema older (or newer) than code | `airflow db migrate` with the matching Airflow version |

---

## See Also

* [Backup and disaster recovery](../07_disaster_recovery/01_backup_and_disaster_recovery.md)
* [Capacity planning](../05_deployment_and_config/05_capacity_planning.md)
* [Upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md)
* [`[database]` reference](../09_reference/01_airflow_cfg_reference.md#database) and [`airflow db` reference](../09_reference/02_cli_reference.md#airflow-db)
