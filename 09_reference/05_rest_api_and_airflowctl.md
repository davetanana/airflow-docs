# REST API v2 and airflowctl

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Airflow public REST API reference](https://airflow.apache.org/docs/apache-airflow/3.4.0/stable-rest-api-ref.html) · [Public API](https://airflow.apache.org/docs/apache-airflow/3.4.0/security/api.html)

The full REST API reference is the OpenAPI document served by your own API server (`/docs` for Swagger UI when `[api] enable_swagger_ui = True`, `/openapi.json` for the schema). This page is the operator's cheat sheet: how to get a token, the dozen calls you use during operations with `curl` examples that work against 3.4, the request-body traps, and `airflowctl`, the API-based CLI. Paths below were checked against `airflow-core/src/airflow/api_fastapi/core_api/routes/public/` on `v3-4-stable`.

---

## 1. Layout of the API Server

| Prefix | What | Auth |
| :--- | :--- | :--- |
| `/api/v2` | Public REST API (stable) | JWT bearer, except `monitor`, `version` |
| `/auth` | Auth manager routes: login, `/auth/token`, `/auth/token/cli`, logout | none (it is the login) |
| `/execution` | Execution API for workers (internal, versioned separately) | task JWT |
| `/ui` | Private API for the React UI. Not stable; don't script against it. | JWT |

`/api/v1` from Airflow 2 does not exist. Validation errors return **422**, not 400.

---

## 2. Getting a Token

`POST /auth/token` with a JSON body returns `201` and `{"access_token": "..."}`. It works with the Simple, FAB and Keycloak auth managers. Tokens live `[api_auth] jwt_expiration_time` (`86400` s). `POST /auth/token/cli` issues shorter tokens (`[api_auth] jwt_cli_expiration_time`, `3600` s).

```bash
export AIRFLOW_URL=http://10.0.0.10:8080
export TOKEN=$(curl -s -X POST "$AIRFLOW_URL/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username": "<user>", "password": "<password>"}' | jq -r .access_token)

curl -s "$AIRFLOW_URL/api/v2/dags?limit=5" -H "Authorization: Bearer $TOKEN" | jq '.dags[].dag_id'
```

Replace `<user>` and `<password>`. Basic auth (Airflow 2's `-u user:pass`) is not accepted by the API server. A token keeps its permissions until it expires: role changes apply to new tokens only.

---

## 3. Most-Used Endpoints

All paths are relative to `$AIRFLOW_URL`. `~` as `dag_id` means "all Dags" on the Dag-run and task-instance list endpoints.

| Task | Method and path |
| :--- | :--- |
| Health (no auth) | `GET /api/v2/monitor/health` |
| Version (no auth) | `GET /api/v2/version` |
| List Dags | `GET /api/v2/dags?limit=100&paused=false&dag_id_pattern=etl` |
| Pause / unpause | `PATCH /api/v2/dags/{dag_id}` body `{"is_paused": true}` |
| Trigger a Dag run | `POST /api/v2/dags/{dag_id}/dagRuns` |
| List Dag runs | `GET /api/v2/dags/{dag_id}/dagRuns?state=failed&order_by=-run_after&limit=20` |
| One Dag run | `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}` |
| Clear a Dag run | `POST /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/clear` |
| Task instances of a run | `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances` |
| Clear task instances | `POST /api/v2/dags/{dag_id}/clearTaskInstances` |
| Task log | `GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}` |
| Import errors | `GET /api/v2/importErrors` |
| Reparse a file | `PUT /api/v2/parseDagFile/{file_token}` |
| Pools | `GET/POST /api/v2/pools`, `GET/PATCH/DELETE /api/v2/pools/{pool_name}` |
| Variables | `GET/POST /api/v2/variables`, `GET/PATCH/DELETE /api/v2/variables/{variable_key}` |
| Connections | `GET/POST /api/v2/connections`, `GET/PATCH/DELETE /api/v2/connections/{connection_id}` |
| Scheduler/triggerer jobs | `GET /api/v2/jobs` |

List endpoints return at most `[api] maximum_page_limit` (`100`) items per page; page with `limit` and `offset`. The response has `total_entries`.

### Trigger a Dag run

```bash
curl -s -X POST "$AIRFLOW_URL/api/v2/dags/nightly_export/dagRuns" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"logical_date": null, "conf": {"target": "staging"}, "note": "manual rerun"}'
```

> [!WARNING]
> `logical_date` has **no default** in `TriggerDAGRunPostBody`: omit it and you get a 422. Send `null` to let Airflow use the run's `run_after` (now), or an ISO-8601 timestamp **with a timezone**. The body is strict: unknown fields such as Airflow 2's `execution_date` are rejected. Other optional fields: `dag_run_id`, `data_interval_start` + `data_interval_end` (both or neither), `run_after`, `partition_key`, `bundle_version`, `drain_dag`.

### Find failed runs across all Dags

```bash
curl -s "$AIRFLOW_URL/api/v2/dags/~/dagRuns?state=failed&order_by=-run_after&limit=20" \
  -H "Authorization: Bearer $TOKEN" | jq -r '.dag_runs[] | [.dag_id, .dag_run_id, .state] | @tsv'
```

### Clear failed task instances

```bash
# 1. Preview: dry_run defaults to TRUE, so this changes nothing
curl -s -X POST "$AIRFLOW_URL/api/v2/dags/nightly_export/clearTaskInstances" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"dag_run_id": "<run_id>", "only_failed": true}' | jq '.task_instances[].task_id'

# 2. Do it
curl -s -X POST "$AIRFLOW_URL/api/v2/dags/nightly_export/clearTaskInstances" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"dag_run_id": "<run_id>", "only_failed": true, "dry_run": false}'
```

> [!IMPORTANT]
> `ClearTaskInstancesBody` defaults: `dry_run = true`, `only_failed = true`, `reset_dag_runs = true`, `include_upstream`/`include_downstream`/`include_past`/`include_future = false`. A clear that "does nothing" usually forgot `"dry_run": false`. Since 3.4, clearing task instances also deletes their task-state-store entries unless you send `"keep_task_state": true`.

### Pools, variables, connections

```bash
# Create a pool (JSON keys: name, slots, description, include_deferred)
curl -s -X POST "$AIRFLOW_URL/api/v2/pools" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"name": "warehouse", "slots": 8}'

# Resize it
curl -s -X PATCH "$AIRFLOW_URL/api/v2/pools/warehouse" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"name": "warehouse", "slots": 16}'

# Create a variable (JSON keys: key, value, description)
curl -s -X POST "$AIRFLOW_URL/api/v2/variables" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"key": "env", "value": "prod"}'

# Create a connection (JSON keys: connection_id, conn_type, host, login, password, schema, port, extra)
curl -s -X POST "$AIRFLOW_URL/api/v2/connections" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"connection_id": "warehouse_db", "conn_type": "postgres", "host": "10.0.0.20", "port": 5432, "login": "etl", "password": "<password>", "schema": "dw"}'
```

Reads of connections return the password masked. Testing a connection through the API is off unless `[core] test_connection` is changed from `Disabled`.

---

## 4. airflowctl

`airflowctl` (distribution `apache-airflow-ctl`, source in `airflow-ctl/`) is a CLI that talks **only** to the REST API, so it needs no database access and no Airflow installation. It is versioned separately from core (0.1.x at the 3.4 release) and needs Python 3.11+.

```bash
pip install apache-airflow-ctl          # or: uv tool install apache-airflow-ctl
airflowctl --help
```

### Log in

```bash
# Username/password: fetches a token from /auth/token/cli and stores it in the system keyring
airflowctl auth login --api-url http://10.0.0.10:8080 --username <user> --password <password> --env prod

# Or bring a token (CI): stored per environment; --skip-keyring writes nothing to the keyring
AIRFLOW_CLI_TOKEN="$TOKEN" airflowctl auth login --api-url http://10.0.0.10:8080 --env ci

airflowctl auth list-envs                # environments and token status
airflowctl auth token --api-url http://10.0.0.10:8080 --username <user> --password <password>   # print a token
```

The environment name defaults to `production`; select another with `--env` at login or with `AIRFLOW_CLI_ENVIRONMENT`. Every command also accepts `--api-token` to bypass the stored token. The environment's config file is kept under `$AIRFLOW_HOME` (default `~/airflow`). `--username`/`--password` cannot be combined with `--skip-keyring`.

### Command groups

Most groups are generated from the API client's operation classes (`airflowctl/api/operations.py`), so their subcommands mirror the REST API; a few hand-written commands are merged in. Run `airflowctl <group> --help` for the exact arguments of your version.

| Group | Typical subcommands |
| :--- | :--- |
| `auth` | `login`, `list-envs`, `token` |
| `dags` | `list`, `get`, `get-details`, `trigger`, `pause`, `unpause`, `drain`, `delete`, `state`, `next-execution`, `clear`, `list-import-errors`, `list-version`, `list-warning` |
| `dagrun` | `list`, `get`, `delete` |
| `taskinstances` | `list`, `get`, `get-dependencies` |
| `tasks` | `clear`, `state`, `states-for-dag-run`, `failed-deps` |
| `pools` | `list`, `get`, `create`, `update`, `delete`, `import`, `export` |
| `variables` | `list`, `get`, `create`, `update`, `delete`, `import` |
| `connections` | `list`, `get`, `create`, `update`, `delete`, `test`, `create-defaults`, `import` |
| `assets` | `list`, `get`, `materialize`, `create-event`, queued-event commands, state-store commands |
| `backfill` | `create`, `create-dry-run`, `list`, `get`, `pause`, `unpause`, `cancel` |
| `config` | `get`, `list`, `lint` |
| `jobs` | `list` (plus `check`) |
| `providers`, `plugins`, `xcom` | read and manage the matching resources |
| `version` | client and server version |

Examples:

```bash
airflowctl dags list --output table
airflowctl dags pause nightly_export
airflowctl dags trigger nightly_export
airflowctl pools export pools.json
airflowctl pools import pools.json
airflowctl variables import variables.json
airflowctl config lint
```

> [!NOTE]
> `airflowctl tasks clear` keeps the API's `dry_run = true` default, so a bare call only previews. Several local `airflow` CLI commands (for example `airflow dags trigger` and `airflow config list`, and most `pools`, `variables` and `connections` subcommands) are marked in source as having an `airflowctl` replacement. They still work in 3.4 and print no deprecation warning, but most of them need direct database access; use `airflowctl` from laptops and CI.

### airflow CLI vs airflowctl

| Need | Use |
| :--- | :--- |
| Start components, `db migrate`, `db clean`, `rotate-fernet-key`, `dags reserialize` | `airflow` on a host with DB access ([CLI reference](02_cli_reference.md)) |
| Day-to-day operations from a laptop or CI without DB credentials | `airflowctl` |
| Scripted integrations | REST API (§3) or the `apache-airflow-client` Python package |

See also: [CLI Operations Guide](../06_operations_and_housekeeping/01_cli_operations_guide.md) and [Security Hardening](../05_deployment_and_config/03_security_hardening.md) for token lifetimes.
