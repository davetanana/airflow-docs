# XCom, Variables, Connections and Secrets Backends

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [XComs](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/xcoms.html) · [Variables](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/variables.html) · [Connections & Hooks](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/connections.html) · [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/3.4.0/security/secrets/secrets-backend/index.html)

The official docs cover XCom, Variables, Connections and secrets backends on separate pages, and a good part of what circulates online still describes Airflow 2. In Airflow 2, tasks read these objects straight from the metadata database. This page covers how they move in 3.4. A task asks its supervisor, the supervisor asks the API server over the Execution API, and secrets backends are consulted in a specific order **in two different places**. The page also covers size limits, the object-storage XCom backend and its off-by-default threshold, URI and JSON connection formats, masking, and caching. Sources are `task-sdk/src/airflow/sdk/execution_time/context.py`, `task-sdk/src/airflow/sdk/bases/xcom.py`, `task-sdk/src/airflow/sdk/execution_time/supervisor.py`, and `airflow-core/src/airflow/config_templates/config.yml` on `v3-4-stable`.

---

## 1. How a Task Reaches These Objects in 3.x

```mermaid
flowchart LR
    T["Task code<br/>Variable.get / Connection.get /<br/>ti.xcom_pull"] --> C["Worker-side chain"]
    C --> B1["1. [workers] secrets_backend<br/>(falls back to [secrets] backend)"]
    B1 -->|miss| B2["2. AIRFLOW_CONN_* / AIRFLOW_VAR_*<br/>in the WORKER's environment"]
    B2 -->|miss| B3["3. ExecutionAPISecretsBackend<br/>→ supervisor → API server"]
    B3 --> S["Server-side chain on the API server"]
    S --> S1["a. [secrets] backend"]
    S1 -->|miss| S2["b. env vars on the API server"]
    S2 -->|miss| S3["c. Metadata DB"]
```

* **Worker chain** (`ensure_secrets_backend_loaded` → `DEFAULT_SECRETS_SEARCH_PATH_WORKERS`): custom backend, then `EnvironmentVariablesBackend`, then `ExecutionAPISecretsBackend`. The custom backend comes from `[workers] secrets_backend` with `[workers] secrets_backend_kwargs`. If that is empty, the worker falls back to `[secrets] backend` and `[secrets] backend_kwargs`.
* **Server chain** (API server, scheduler, Dag processor in server context): `[secrets] backend`, then `EnvironmentVariablesBackend`, then `MetastoreBackend`.
* If a backend raises `AirflowSecretsBackendAccessDenied`, the lookup stops: a deny is never passed on to a less restrictive backend. Any other exception is logged, and the next backend is tried.
* A missing connection raises `AirflowNotFoundException: The conn_id '<id>' isn't defined`. A missing variable without a `default` raises `AirflowRuntimeError` with `VARIABLE_NOT_FOUND`.

> [!IMPORTANT]
> Because the worker calls the external backend **directly** and only falls back to the API server on a miss, workers need credentials and network access to Vault, AWS Secrets Manager, or whichever store you use, unless you leave `[workers] secrets_backend` and `[secrets] backend` unset in the worker's config. To keep secret-store credentials off workers, configure the backend only on the API server, and remove it from the worker config.

XComs follow a similar path. `ti.xcom_push` / `xcom_pull` and returned values go to the supervisor, then to the API server, then to the `xcom` table. A custom XCom backend (§3) runs **in the task process**, so it needs its own storage credentials on the worker.

---

## 2. XCom Essentials and Size Limits

* A task's return value is pushed under key `return_value` (`BaseXCom.XCOM_RETURN_KEY`). `multiple_outputs=True` on `@task` pushes each dict key separately.
* Keys are at most 512 characters (`key` column is `String(512)`).
* Values are serialized with `airflow.sdk.serde` into JSON. There is no pickling in 3.x. Anything serde cannot handle fails with an error at push time.
* The `value` column is `JSON` (`JSONB` on PostgreSQL). There is no Airflow-side size check in the 3.4 source; the limit is the database's.

| Backend DB | Practical limit for one XCom value | Source |
| :--- | :--- | :--- |
| PostgreSQL (`JSONB`) | About 255 MB per value | PostgreSQL limits *(not verified against Airflow source)* |
| MySQL (`JSON`) | Bounded by the server's `max_allowed_packet` | MySQL limits *(not verified against Airflow source)* |
| SQLite (`JSON` as text) | About 1 GB (`SQLITE_MAX_LENGTH`) | SQLite limits *(not verified against Airflow source)* |

These are hard ceilings, not targets. Every XCom also travels through the Execution API as JSON and is held in memory by the API server, the supervisor and the task *(illustrative)*. Keep values in the database small (IDs, paths, small dicts), and send anything above a few hundred kilobytes to object storage.

```python
from airflow.sdk import dag, task


@dag(schedule=None)
def xcom_demo():
    @task(multiple_outputs=True)
    def extract() -> dict:
        return {"path": "s3://example-bucket/raw/2026-01-01.parquet", "rows": 10_000}

    @task
    def load(path: str, rows: int):
        print(f"loading {rows} rows from {path}")

    out = extract()
    load(out["path"], out["rows"])


xcom_demo()
```

Pull explicitly in classic operators or templates with `ti.xcom_pull(task_ids="extract", key="return_value")` or `{{ ti.xcom_pull(task_ids='extract') }}`. Always pass `task_ids`.

`[core] max_map_length` (default `1024`) caps the length of a list or dict pushed to drive dynamic task mapping. The pushing task fails if it is longer.

---

## 3. Custom XCom Backends

Set `[core] xcom_backend` (default `airflow.sdk.execution_time.xcom.BaseXCom`) to a subclass of `airflow.sdk.BaseXCom`. Anything that is not a subclass raises `TypeError: Your custom XCom class ... is not a subclass of BaseXCom`. Set it on **every** component that reads XComs: workers, triggerer, API server (UI display) and Dag processor.

### Object-storage backend (`apache-airflow-providers-common-io`)

```ini
[core]
xcom_backend = airflow.providers.common.io.xcom.backend.XComObjectStorageBackend

[common.io]
xcom_objectstorage_path = s3://aws_default@example-bucket/xcom
xcom_objectstorage_threshold = 1048576
xcom_objectstorage_compression = gzip
```

| Key (`[common.io]`) | Default | Meaning |
| :--- | :--- | :--- |
| `xcom_objectstorage_path` | `""` | `scheme://conn_id@bucket/prefix` |
| `xcom_objectstorage_threshold` | `-1` | `-1` keeps everything in the DB, `0` sends everything to object storage, `N` sends values of `N` bytes or more |
| `xcom_objectstorage_compression` | `""` | e.g. `gzip`, `bz2`, `lzma`, `snappy` (needs `python-snappy`) |

> [!WARNING]
> With the default threshold of `-1`, switching `xcom_backend` to the object-storage backend **changes nothing**: every value still goes to the database. Set a threshold.

### Writing your own (3.4 API)

In 3.4 the hooks to override are `store_value(cls, coordinate, key, value)` and `load_value(cls, stored)`. Use `XComCoordinate.build_path()` to get a storage path that is unique per task instance, including loop iterations and map indexes:

```python
from __future__ import annotations

import json
from typing import Any

from airflow.sdk import BaseXCom, ObjectStoragePath
from airflow.sdk.bases.xcom import XComCoordinate


class JsonFileXCom(BaseXCom):
    PREFIX = "s3://aws_default@example-bucket/xcom"
    MARKER = "__json_file_xcom__"

    @classmethod
    def store_value(cls, coordinate: XComCoordinate, key: str, value: Any):
        data = BaseXCom.serialize_value(value)
        path = ObjectStoragePath(coordinate.build_path(cls.PREFIX, key, suffix=".json"))
        path.write_text(json.dumps(data))
        return {cls.MARKER: str(path)}  # this small dict is what the xcom table stores

    @classmethod
    def load_value(cls, stored):
        if isinstance(stored, dict) and cls.MARKER in stored:
            stored = json.loads(ObjectStoragePath(stored[cls.MARKER]).read_text())
        return super().load_value(stored)
```

Rows written by the REST API or by an earlier backend still reach `load_value`, so always fall back to `super()`. Override `purge()` if objects should be deleted when the XCom row is cleared. The older `serialize_value` / `deserialize_value` overrides still work; `BaseXCom` picks whichever one you overrode.

---

## 4. Variables from Tasks

```python
from airflow.sdk import Variable, task


@task
def use_vars():
    env = Variable.get("environment", default="dev")
    limits = Variable.get("limits", deserialize_json=True)
    Variable.set("last_run_marker", "ok")          # writes through the Execution API
    keys = Variable.keys(prefix="feature_")        # metadata DB only, see below
    return env, limits, keys
```

Templates: `{{ var.value.environment }}`, `{{ var.json.limits.max_rows }}`.

Traps:
* `Variable.set` writes to the **metadata database**. If a secrets backend or an `AIRFLOW_VAR_<KEY>` variable defines the same key, reads keep returning that value, because those come earlier in the chain.
* `Variable.keys()` lists keys from the metadata database only (the Execution API route selects from the `variable` table). Keys that exist only in a secrets backend or in environment variables are not listed.
* `Variable.get()` at the **top level of a Dag file** runs on every parse. `[secrets] use_cache` (default `False`) and `[secrets] cache_ttl_seconds` (default `900`) cache Variables and Connections, but only in the Dag processor (`SecretCache.init()` is called only from `dag_processing/manager.py`). Task runs are never cached.

---

## 5. Connections from Tasks

```python
from airflow.sdk import BaseHook, Connection, task


@task
def use_conn():
    conn = Connection.get("warehouse")          # or BaseHook.get_connection("warehouse")
    return conn.host, conn.port, conn.extra_dejson.get("region")
```
Template: `{{ conn.warehouse.host }}`.

### Defining connections in environment variables

Variable name: `AIRFLOW_CONN_<CONN_ID upper-cased>`. In multi-team mode, a team-scoped form `AIRFLOW_CONN__<TEAM>___<CONN_ID>` is checked first (the same pattern applies to `AIRFLOW_VAR_`). The value can be a URI or, if it starts with `{`, JSON:

```bash
# URI form: special characters in user/password must be URL-encoded; extras go in the query string
export AIRFLOW_CONN_WAREHOUSE='postgres://etl_user:s3cr%40t@10.0.0.15:5432/analytics?sslmode=require'

# JSON form: no encoding headaches; "extra" may be an object
export AIRFLOW_CONN_WAREHOUSE='{"conn_type": "postgres", "host": "10.0.0.15", "port": 5432, "login": "etl_user", "password": "s3cr@t", "schema": "analytics", "extra": {"sslmode": "require"}}'

export AIRFLOW_VAR_ENVIRONMENT=prod
```

> [!NOTE]
> Environment-variable connections and variables are read from the environment of the process doing the lookup. For a task, that is the **worker** first, then the API server's environment on a miss. Setting `AIRFLOW_CONN_*` only on the scheduler does nothing for tasks. Such connections do not appear in `airflow connections list` or the UI, which read the database.

Add one to the database instead:
```bash
airflow connections add warehouse --conn-json '{"conn_type": "postgres", "host": "10.0.0.15", "port": 5432, "login": "etl_user", "password": "<password>", "schema": "analytics"}'
airflow connections get warehouse
```
`airflow connections test <conn_id>` only works when `[core] test_connection` is enabled (default `Disabled`).

---

## 6. Secrets Backends

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "connections", "variables_path": "variables", "mount_point": "airflow", "url": "https://vault.example.internal:8200"}
```

* `backend_kwargs` must be a JSON object. If it does not parse, the backend starts with **no** kwargs and logs `Failed to parse [secrets] backend_kwargs as JSON, defaulting to no kwargs.`, which is easy to miss.
* Each kwarg can also be set as its own environment variable, which overrides the same key in the JSON: `AIRFLOW__SECRETS__BACKEND_KWARG__<KEY>` for `[secrets]`, and `AIRFLOW__WORKERS__SECRETS_BACKEND_KWARG__<KEY>` for `[workers]`. Values are raw strings, not parsed as JSON. Use this to inject a token from a Kubernetes Secret without rewriting the whole JSON.
* `[workers] secrets_backend` / `secrets_backend_kwargs` let workers use a different backend or credentials (for example a read-only role).
* Only one custom backend per chain. Backend instances are cached per process once a custom backend is configured.

| Process | Chain |
| :--- | :--- |
| Task runner on a worker | `[workers] secrets_backend` (or `[secrets] backend`) → worker env vars → Execution API |
| API server (serving the Execution API, UI, REST) | `[secrets] backend` → API server env vars → metadata DB |
| Task supervisor and other processes outside both contexts | custom backend → env vars only (no metadata DB, no Execution API) |

---

## 7. Masking Sensitive Values

* Connection passwords are always masked in task logs. Connection extras and Variables are masked when their **key name** contains a sensitive word and `[core] hide_sensitive_var_conn_fields` is `True` (default).
* Built-in sensitive words (`DEFAULT_SENSITIVE_FIELDS` in the shared secrets masker) include `password`, `passwd`, `secret`, `token`, `access_token`, `api_key`, `apikey`, `private_key`, `authorization`, `bearer`, `dsn`, `connection_string`, `keyfile_dict`, `service_account`, `webhook_url`. Add your own with `[core] sensitive_var_conn_names` (comma-separated).
* Values shorter than `[logging] min_length_masked_secret` (default `5`) are **not** masked, so a 4-character password appears in clear text.
* Mask anything else yourself:

```python
from airflow.sdk import task
from airflow.sdk.log import mask_secret


@task
def call_api():
    token = fetch_token()  # your function
    mask_secret(token)
    ...
```

> [!WARNING]
> Masking only applies to log output. A secret returned from a task becomes an XCom stored in clear text in the database (or your object store) and visible in the UI. Never return secrets from tasks.

---

## 8. Quick Diagnosis

| Symptom | Likely cause | Check |
| :--- | :--- | :--- |
| `The conn_id 'x' isn't defined` on the worker, but it is in the UI | A custom backend on the worker raised `AirflowSecretsBackendAccessDenied`; or the API server's backend has a different prefix | Worker log at DEBUG shows each backend tried |
| Variable change not picked up in Dag structure | `[secrets] use_cache` with `cache_ttl_seconds` | Wait for the TTL, or turn the cache off |
| `Variable.set` value "ignored" | Same key in a secrets backend or `AIRFLOW_VAR_` | Remove the shadowing definition |
| XCom push fails on a custom object | Not serializable by serde | Return plain types or a dataclass |
| Large XComs slow the UI or the DB | Values stored in the `xcom` table | Object-storage backend with a threshold, then `airflow db clean` (see [Housekeeping](../06_operations_and_housekeeping/02_housekeeping_and_automations.md)) |

---

**Next:** [Installation and Python](../04_infrastructure_and_cloud/01_installation_and_python.md). See also [Security hardening](../05_deployment_and_config/03_security_hardening.md), [Task SDK quick reference](../09_reference/06_task_sdk_quick_reference.md), and the [`[secrets]` reference](../09_reference/01_airflow_cfg_reference.md#secrets).
