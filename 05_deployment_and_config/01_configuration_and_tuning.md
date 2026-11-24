# Configuration and Tuning

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Setting Configuration Options](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/set-config.html) · [Configuration Reference](https://airflow.apache.org/docs/apache-airflow/3.4.0/configurations-ref.html) · [Performance tuning](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/performance.html)

The official configuration reference lists every option. This page adds three things it does not give you in one place: how Airflow 3.4 actually **resolves** a value (taken from the shared configuration parser, not from the prose docs), the **traps** in the CLI and in auto-generated secrets, and a short list of the keys that matter in production, grouped by component, with their 3.4 defaults from `config.yml`. For every key and default, the generated [airflow.cfg reference](../09_reference/01_airflow_cfg_reference.md) is the arbiter.

---

## 1. How a Value Is Resolved

Every component reads the same option through one lookup chain (`AirflowConfigParser._lookup_sequence` in `shared/configuration/src/airflow_shared/configuration/parser.py`). The first source that returns a value wins:

| Order | Source | Example |
| :--- | :--- | :--- |
| 1 | Environment variable | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://...` |
| 2 | Environment variable with `_CMD` (sensitive keys only) | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_CMD="cat /run/secrets/db_url"` |
| 3 | Environment variable with `_SECRET` (sensitive keys only) | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_SECRET=sql_alchemy_conn` |
| 4 | `airflow.cfg` plain value | `sql_alchemy_conn = ...` under `[database]` |
| 5 | `airflow.cfg` `<key>_cmd` (sensitive keys only) | `sql_alchemy_conn_cmd = cat /run/secrets/db_url` |
| 6 | `airflow.cfg` `<key>_secret` (sensitive keys only) | `sql_alchemy_conn_secret = sql_alchemy_conn` |
| 7 | Built-in default from `config.yml` | `sqlite:///{AIRFLOW_HOME}/airflow.db` |
| 8 | Provider defaults (provider `provider.yaml` / fallback cfg) | `[celery] worker_concurrency = 16` |

Rules worth knowing:

* **Environment variable names** are `AIRFLOW__{SECTION}__{KEY}`, upper-cased, double underscores. In multi-team mode a team-scoped override is `AIRFLOW__{TEAM}___{SECTION}__{KEY}` (three underscores after the team name).
* **`_CMD` and `_SECRET` only work for keys marked `sensitive: true`.** For any other key the parser silently ignores them; it does not fail. In 3.4 core these are: `[core] fernet_key`, `[core] asset_manager_kwargs`, `[database] sql_alchemy_conn`, `[database] sql_alchemy_conn_async`, `[database] sql_alchemy_engine_args`, `[logging] remote_task_handler_kwargs`, `[secrets] backend_kwargs`, `[api] secret_key`, `[workers] secrets_backend_kwargs`, `[api_auth] jwt_secret`, `[sentry] sentry_dsn`. Providers add their own, for example `[celery] broker_url`, `[celery] result_backend` and `[celery] flower_basic_auth`.
* **`_SECRET` reads from the configured `[secrets] backend`**, using that backend's config prefix, not its connection prefix.
* **Secrets-backend kwargs can be set one key at a time**: `AIRFLOW__SECRETS__BACKEND_KWARG__role_id=...` becomes `{"role_id": "..."}` (the part after the prefix is lower-cased, so `ROLE_ID` works too). The worker equivalent is `AIRFLOW__WORKERS__SECRETS_BACKEND_KWARG__role_id=...`. Values are raw strings, not parsed JSON.
* **Config file location**: `$AIRFLOW_CONFIG`, else `$AIRFLOW_HOME/airflow.cfg`, else `~/airflow/airflow.cfg`. If the file does not exist, the first Airflow process creates it with every default written out, and the file is made unreadable to group and other.

> [!WARNING]
> **Auto-generated secrets differ per process.** When `airflow.cfg` is first created, Airflow generates a Fernet key and a `[api_auth] jwt_secret` and writes them into the file. If you run components in separate containers, each with its own freshly created file (or with no file and only environment variables), each process ends up with **its own** values:
> * `[api_auth] jwt_secret` empty: each process generates a random key at startup and logs `` `api_auth/jwt_secret` was empty, using a generated one for now``. Tokens signed by the scheduler then fail validation on the API server.
> * `[api] secret_key` defaults to a value generated per process (`SECRET_KEY` in `airflow/configuration.py`). The API server can then no longer fetch logs from worker and triggerer log servers.
> * `[core] fernet_key` empty: connections and variables are stored **unencrypted** (`get_fernet()` logs `empty cryptography key - values will not be stored encrypted.`).
>
> Set all three explicitly, identically, on the components that need them. See [Security Hardening](03_security_hardening.md#2-secrets-and-keys-that-must-be-set).

---

## 2. Inspecting and Migrating Config with the CLI

The `airflow config` group has four subcommands in 3.4: `get-value`, `list`, `lint` and `update`.

```bash
# One value, after all resolution steps (env, cfg, _cmd, _secret, defaults, providers)
airflow config get-value core executor

# Option names only - values are hidden by default in 3.4
airflow config list --section scheduler

# Values, with the source of each one, sensitive values masked
airflow config list --section database --show-values --include-sources --hide-sensitive

# Print the env var name next to each option
airflow config list --section api --show-values --include-env-vars

# Generate a reference config of defaults (needs --show-values, see trap below)
airflow config list --defaults --show-values > /tmp/airflow-defaults.cfg
```

> [!WARNING]
> Traps in `airflow config`:
> * `airflow config list` **hides values unless you pass `--show-values`**. With `--defaults` but without `--show-values`, every option is written as an empty comment (`# option = `), so the output is useless as a template.
> * `airflow config get-value` prints **nothing and exits successfully** when the section or option does not exist. A typo looks like an empty value. Check the name in the [reference](../09_reference/01_airflow_cfg_reference.md).
> * `get-value` loads provider configuration, so it is slow; do not call it in a tight loop.

For Airflow 2 configs, `lint` and `update` report keys that were renamed, removed, or changed default in Airflow 3:

```bash
airflow config lint                                   # report problems
airflow config update                                 # dry run: breaking changes only
airflow config update --all-recommendations           # dry run: breaking + recommended
airflow config update --fix                           # rewrite airflow.cfg (a backup is written first)
airflow config update --fix --section core,database   # limit to sections
```

`update --fix` strips the comments from `airflow.cfg`. It does not touch environment variables, Helm values or secrets, so audit those by hand. See [Upgrading to Airflow 3](04_upgrading_to_airflow_3.md).

---

## 3. Core and Database

| Key | 3.4 default | Production guidance |
| :--- | :--- | :--- |
| `[core] executor` | `LocalExecutor` | `CeleryExecutor` or `KubernetesExecutor` for multi-host. A comma-separated list enables several executors. See [Executors](../03_task_execution/01_executors.md). |
| `[core] parallelism` | `32` | Max running task instances **per scheduler**. Total = value × number of schedulers. |
| `[core] max_active_tasks_per_dag` | `16` | Per Dag run. Raise only for wide Dags. |
| `[core] max_active_runs_per_dag` | `16` | Lower it for Dags that must not overlap. |
| `[core] load_examples` | `True` | **Set `False`** in every non-dev deployment. |
| `[core] dags_are_paused_at_creation` | `True` | Keep. New Dags start paused. |
| `[core] fernet_key` | generated into a new `airflow.cfg` | Set explicitly; same value on every component. |
| `[core] dagbag_import_timeout` | `30.0` | Per-file import timeout during parsing. |
| `[core] auth_manager` | `...simple.simple_auth_manager.SimpleAuthManager` | **Development only.** Use the FAB or Keycloak provider. See [Security Hardening](03_security_hardening.md). |
| `[core] execution_api_server_url` | unset → `{[api] base_url}/execution/`, or `http://localhost:{[api] port}/execution/` | **Must be set** (or `[api] base_url` must be absolute) on any worker that is not on the API server host. |
| `[core] min_serialized_dag_update_interval` | `30` | Floor for how often a serialized Dag is rewritten. |
| `[core] test_connection` | `Disabled` | Leave disabled unless you trust everyone who can edit connections. |
| `[database] sql_alchemy_conn` | `sqlite:///{AIRFLOW_HOME}/airflow.db` | PostgreSQL or MySQL in production. In 3.4, bare `postgresql://` is rewritten to the psycopg 3 driver. |
| `[database] sql_alchemy_pool_size` | `5` | Per **process** pool. See [Capacity Planning](05_capacity_planning.md#4-metadata-database-connections). |
| `[database] sql_alchemy_max_overflow` | `10` | Max connections per process = pool_size + max_overflow. |
| `[database] sql_alchemy_pool_recycle` | `1800` | Lower it if a proxy or firewall drops idle connections sooner. |
| `[database] sql_alchemy_pool_pre_ping` | `True` | Keep. |
| `[database] check_migrations` | `True` | Components check the schema at startup. |
| `[database] migration_batch_size` | `10000` | Rows per batch in data migrations. |

---

## 4. Scheduler

| Key | 3.4 default | What it does / when to change |
| :--- | :--- | :--- |
| `[scheduler] scheduler_heartbeat_sec` | `5` | Heartbeat interval of the scheduler job. |
| `[scheduler] scheduler_idle_sleep_time` | `1` | Sleep between loops only when the loop did nothing. |
| `[scheduler] max_tis_per_query` | `16` | Task instances examined per loop. `0` means use `[core] parallelism`. Raise it if `scheduler.tasks.executable` stays high while slots are free. |
| `[scheduler] max_dagruns_to_create_per_loop` | `10` | Dag runs created per loop. |
| `[scheduler] max_dagruns_per_loop_to_schedule` | `20` | Dag runs examined and locked per loop. |
| `[scheduler] use_row_level_locking` | `True` | Must stay `True` to run more than one scheduler. |
| `[scheduler] task_queued_timeout` | `600.0` | Queued tasks older than this are retried or failed. It is also the lifetime of the workload token sent to the executor queue. |
| `[scheduler] task_instance_heartbeat_timeout` | `300` | A running task with no heartbeat for this long is failed. |
| `[scheduler] catchup_by_default` | `False` | Airflow 3 default. Set `catchup=True` per Dag when needed. |
| `[scheduler] create_cron_data_intervals` | `False` | `False` means a cron string creates a `CronTriggerTimetable`. |
| `[scheduler] dag_cache_size` | `512` | Deserialized Dag versions the scheduler keeps in memory. |
| `[scheduler] enable_health_check` | `False` | Set `True` to serve `/health` on `[scheduler] scheduler_health_check_server_port` (`8974`). |
| `[scheduler] scheduler_health_check_threshold` | `30` | Seconds without a heartbeat before the scheduler counts as unhealthy. |
| `[scheduler] batch_asset_events` | `False` | 3.4 default: one Dag run per asset event for single-asset schedules. |

The Airflow 2 switch for a standalone Dag processor and the parsing keys are no longer in this section: in Airflow 3 the Dag processor is always a separate process (§5).

---

## 5. Dag Processor

The Dag processor is configured in `[dag_processor]`, not `[scheduler]`. Several of these keys lived in `[scheduler]` in 2.x.

| Key | 3.4 default | What it does |
| :--- | :--- | :--- |
| `[dag_processor] dag_bundle_config_list` | one `LocalDagBundle` named `dags-folder` | Where Dags come from. See [Deployment Runbook](02_deployment_runbook.md#3-dag-bundles). |
| `[dag_processor] refresh_interval` | `300` | How often a bundle is refreshed and listed for new files. A per-bundle `refresh_interval` kwarg overrides it. |
| `[dag_processor] min_file_process_interval` | `30` | Minimum seconds between re-parses of one file. |
| `[dag_processor] parsing_processes` | `2` | Parallel parser processes per Dag processor. |
| `[dag_processor] dag_file_processor_timeout` | `50` | Kill a file's parse after this many seconds. |
| `[dag_processor] stale_dag_threshold` | `50` | Grace period before Dags missing from their file are deactivated. |
| `[dag_processor] file_parsing_sort_mode` | `modified_time` | Use `random_seeded_by_host` when several processors share a bundle. |
| `[dag_processor] dag_bundle_storage_path` | unset → `<tmpdir>/airflow/dag_bundles` | Set it to a persistent volume for versioned bundles. |
| `[dag_processor] bundle_refresh_check_interval` | `5` | How often the processor checks whether a bundle is due for refresh. |
| `[dag_processor] stale_bundle_cleanup_interval` | `1800` | How often old bundle versions are cleaned up. |
| `[dag_processor] print_stats_interval` | `30` | Parsing stats in the processor log. `0` disables. |
| `[dag_processor] parsing_pre_import_modules` | `True` | Pre-imports Airflow modules before forking parsers. |

> [!TIP]
> Parse cost is per file. `[core] dagbag_import_timeout` (30 s) caps the import of one file. `[dag_processor] dag_file_processor_timeout` (50 s) caps the whole parse process. Keep the second larger than the first. Parsing capacity math is in [Capacity Planning](05_capacity_planning.md#3-dag-processor-parsing-capacity).

---

## 6. API Server, Execution API and Workers

| Key | 3.4 default | Notes |
| :--- | :--- | :--- |
| `[api] host` / `[api] port` | `0.0.0.0` / `8080` | Replace the Airflow 2 webserver host and port keys. |
| `[api] workers` | `1` | Prefer more API server replicas over more workers per replica. |
| `[api] worker_timeout` | `120` | Seconds. |
| `[api] server_type` | `uvicorn` | `gunicorn` (needs the `gunicorn` extra) allows `[api] worker_refresh_interval` rolling worker restarts and copy-on-write memory sharing. |
| `[api] base_url` | unset | Set the public URL, for example `https://airflow.example.internal`. |
| `[api] secret_key` | generated per process | Set explicitly; shared by the API server, scheduler, workers and triggerer (log fetch tokens). |
| `[api] expose_config` | `False` | Keep `False`. |
| `[api] enable_swagger_ui` | `True` | Disable on internet-facing deployments if you don't need it. |
| `[api] ssl_cert` / `[api] ssl_key` | empty | TLS directly on the API server. |
| `[api] maximum_page_limit` | `100` | Cap on `limit` in list endpoints. |
| `[api_auth] jwt_secret` | generated into a new `airflow.cfg` | See §1 warning. |
| `[api_auth] jwt_expiration_time` | `86400` | UI and REST API token lifetime. |
| `[api_auth] jwt_cli_expiration_time` | `3600` | Tokens from `/auth/token/cli`. |
| `[execution_api] jwt_expiration_time` | `600` | Task token lifetime (refreshed automatically). |
| `[workers] min_heartbeat_interval` | `5` | Worker heartbeat to the Execution API. |
| `[workers] max_failed_heartbeats` | `30` | The whole retry budget in 3.4 (it was `3`). About 150 s of API outage before a task is killed. |
| `[workers] execution_api_timeout` | `5.0` | Per request. |
| `[workers] secrets_backend` | empty | A worker-side secrets backend, checked before environment variables. |

---

## 7. Triggerer

| Key | 3.4 default | Notes |
| :--- | :--- | :--- |
| `[triggerer] capacity` | `1000` | Triggers per triggerer process. Override with `airflow triggerer --capacity`. |
| `[triggerer] max_trigger_to_select_per_loop` | `50` | Keep well below `capacity` so HA triggerers balance load. |
| `[triggerer] triggerer_health_check_threshold` | `30` | Used by `airflow jobs check --job-type TriggererJob`. |
| `[triggerer] blocked_main_thread_warning_threshold` | `0.2` | Seconds. A trigger blocking the event loop longer than this is reported. |
| `[triggerer] queues_enabled` | `False` | Lets triggerers be assigned trigger queues (`airflow triggerer --queues`). |
| `[logging] trigger_log_server_port` | `8794` | Triggerer log server; must be reachable from the API server. |

---

## 8. Logging and Metrics

| Key | 3.4 default | Notes |
| :--- | :--- | :--- |
| `[logging] base_log_folder` | `{AIRFLOW_HOME}/logs` | Local task logs. |
| `[logging] remote_logging` | `False` | Turn on for any multi-host deployment. |
| `[logging] remote_base_log_folder` / `remote_log_conn_id` | empty | For example an object-store URL plus a connection ID. |
| `[logging] delete_local_logs` | `False` | `True` once remote logging is confirmed working. |
| `[logging] logging_level` | `INFO` | |
| `[logging] json_logs` | `False` | Structured logs for log shippers. |
| `[logging] worker_log_server_port` | `8793` | Worker log server for live logs. |
| `[logging] min_length_masked_secret` | `5` | Secrets shorter than this are not masked. |
| `[metrics] statsd_on` / `[metrics] otel_on` | `False` / `False` | Enable one of them. See [Monitoring](../06_operations_and_housekeeping/03_monitoring_and_observability.md). |

Details on remote logging are in [Logging](../06_operations_and_housekeeping/04_logging.md).

---

## 9. Settings to Review Before Go-Live

| # | Setting | Default | Why it matters |
| :--- | :--- | :--- | :--- |
| 1 | `[database] sql_alchemy_conn` | SQLite | SQLite cannot run more than one scheduler or real workloads. |
| 2 | `[core] fernet_key` | per new cfg file | Empty → secrets stored unencrypted; mismatched → `InvalidToken` on read. |
| 3 | `[api_auth] jwt_secret` (or `jwt_private_key_path`) | per new cfg file | Must match between scheduler and API server, or every task fails to authenticate. |
| 4 | `[api] secret_key` | per process | Must match, or log viewing from workers fails. |
| 5 | `[core] auth_manager` | `SimpleAuthManager` | Dev-only manager. |
| 6 | `[core] load_examples` | `True` | Example Dags clutter production and the parse loop. |
| 7 | `[core] execution_api_server_url` / `[api] base_url` | unset | Remote workers otherwise call `http://localhost:8080/execution/`. |
| 8 | `[logging] remote_logging` | `False` | Logs disappear with ephemeral workers. |
| 9 | `[dag_processor] dag_bundle_config_list` | local Dags folder | Choose LocalDagBundle on shared storage, or GitDagBundle. |
| 10 | `[core] parallelism`, `[celery] worker_concurrency` | `32`, `16` | Size them together; see [Capacity Planning](05_capacity_planning.md). |
| 11 | `[database] sql_alchemy_pool_size` / `max_overflow` | `5` / `10` | Total connections across all processes must fit the database or PgBouncer. |
| 12 | `[metrics] statsd_on` or `otel_on` | `False` | You cannot tune what you do not measure. |
| 13 | `[scheduler] enable_health_check` | `False` | Needed for HTTP liveness probes on the scheduler. |
| 14 | `[api] expose_config` | `False` | Keep it off. |

Next: put the components on hosts with the [Deployment Runbook](02_deployment_runbook.md), then lock them down with [Security Hardening](03_security_hardening.md).
