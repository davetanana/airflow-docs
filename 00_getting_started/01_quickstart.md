# Quickstart: Your First 15 Minutes with Airflow 3.4

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Quick Start](https://airflow.apache.org/docs/apache-airflow/3.4.0/start.html) · [Installation from PyPI](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/installing-from-pypi.html) · [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/docker-compose/index.html)

This page takes you from nothing to a running Airflow 3.4 on your laptop, a first Dag written with the Task SDK, a manual run, its logs, and a local test. The official "Quick Start" gets you to the login screen. This page goes further: it says what `airflow standalone` actually starts and overrides, why a new Dag file can take minutes to appear, and which 2.x habits break on 3.4. Each step links to the page that explains it in depth.

> [!WARNING]
> This is a **development** setup: SQLite, the Simple auth manager with a generated password, every component in one terminal. `airflow standalone` itself prints "Do not use this in production". For a real deployment, follow the [deployment runbook](../05_deployment_and_config/02_deployment_runbook.md) and [security hardening](../05_deployment_and_config/03_security_hardening.md).

---

## 1. What you need

* Python **3.11, 3.12, 3.13 or 3.14**. `airflow-core/pyproject.toml` in 3.4.0 declares `requires-python = ">=3.11,!=3.15"`. Python 3.10 and older no longer install.
* macOS, Linux, or Windows through WSL2. Airflow does not run natively on Windows.
* Port 8080 free. That is the default `[api] port`.
* Network access to `raw.githubusercontent.com` for the constraints file.

Airflow keeps all of its local state in `AIRFLOW_HOME`, which defaults to `~/airflow`. Pick one value and export it in **every** terminal you use; a terminal without it silently uses `~/airflow`, with a different database and a different admin password.

```bash
export AIRFLOW_HOME=~/airflow
```

---

## 2. Install with the constraints file

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip

AIRFLOW_VERSION=3.4.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

`uv` works the same way: `uv venv`, then `uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`.

Why the constraints file matters: `apache-airflow` declares loose lower bounds on hundreds of transitive dependencies. Without the constraints file, pip resolves whatever is newest today, which is not what the release was tested with, and installs that worked last week can break. A 404 on the constraints URL means your Python version is not supported by that release. See [installation & Python](../04_infrastructure_and_cloud/01_installation_and_python.md) for extras, providers and upgrades.

---

## 3. Start everything with `airflow standalone`

```bash
airflow standalone
```

What it does, from `airflow-core/src/airflow/cli/commands/standalone_command.py`:

| Step | Detail |
| :--- | :--- |
| Creates the database | Calls `db.initdb()`, which creates or migrates the schema in `[database] sql_alchemy_conn`, default `sqlite:///{AIRFLOW_HOME}/airflow.db`. |
| Forces a local executor | If `[core] executor` is not a local executor, it sets `AIRFLOW__CORE__EXECUTOR=LocalExecutor` for its children. The 3.4 default is already `LocalExecutor`. |
| Forces the Simple auth manager | If `[core] auth_manager` is anything else (for example the FAB auth manager), it overrides it with `SimpleAuthManager`. |
| Speeds up Dag discovery | If you left `[dag_processor] refresh_interval` and `[dag_processor] dag_bundle_config_list` at their defaults, it sets `AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL` to `[dag_processor] bundle_refresh_check_interval` (default `5`) so new files show up within seconds. See §5. |
| Starts four processes | `scheduler`, `dag-processor`, `api-server`, `triggerer`, each as `airflow <component>`, with output prefixed by the component name. |
| Prints "Airflow is ready" | Only once port `[api] port` answers **and** the scheduler, Dag processor and triggerer all have a live heartbeat in the `job` table. |

`Ctrl+C` stops all four.

### Where the admin password comes from

The Simple auth manager reads its users from `[core] simple_auth_manager_users` (default `admin:admin`, meaning user `admin` with role `admin`). Passwords are not in config. On first start the auth manager generates one per user, prints a line like `Password for user 'admin': <generated-password>`, and writes it in plain text to:

```bash
cat "${AIRFLOW_HOME:-$HOME/airflow}/simple_auth_manager_passwords.json.generated"
```

On later starts, standalone prints only that the password "has been previously generated" and the file path. The file location can be changed with `[core] simple_auth_manager_passwords_file`. If you already set `[core] simple_auth_manager_users` to something other than `admin:admin`, standalone prints nothing about passwords; read the file. If `[core] simple_auth_manager_all_admins` is `True`, there is no login at all and every request is treated as an admin. Never use that outside a laptop.

Open `http://localhost:8080` and log in as `admin`.

> [!NOTE]
> A warning such as `SimpleAuthManager is active but the deployment shape looks like production` is expected here: the API server binds `[api] host` = `0.0.0.0` by default, which the check treats as non-local. Don't silence it by changing the auth manager on a laptop; take it seriously everywhere else.

### Alternative: the official Docker Compose file

The repository ships `airflow-core/docs/howto/docker-compose/docker-compose.yaml`. It is closer to a multi-process deployment: PostgreSQL 16, Redis, `CeleryExecutor`, the FAB auth manager, and separate `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker` and `airflow-triggerer` containers. The copy in the source tree contains a `|version|` placeholder, so download the rendered file from the docs site instead:

```bash
mkdir airflow-compose && cd airflow-compose
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.4.0/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up
```

Log in with `airflow` / `airflow` (from `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`). Your Dags go in `./dags`. `docker compose down --volumes --remove-orphans` deletes everything, including the database volume. The rest of this page assumes `airflow standalone`; with Compose, prefix CLI commands with `docker compose run airflow-cli`. See [Docker images](../04_infrastructure_and_cloud/02_docker_images.md).

---

## 4. Write a first Dag

Open a second terminal, activate the virtual environment, export the same `AIRFLOW_HOME`, and create the Dags folder (`[core] dags_folder`, default `{AIRFLOW_HOME}/dags`):

```bash
mkdir -p "$AIRFLOW_HOME/dags"
```

Save this as `$AIRFLOW_HOME/dags/hello_airflow.py`:

```python
import pendulum

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["quickstart"],
)
def hello_airflow():
    @task
    def extract() -> list[int]:
        return [3, 1, 4, 1, 5]

    @task
    def summarize(numbers: list[int]) -> int:
        total = sum(numbers)
        print(f"Sum of {len(numbers)} numbers is {total}")
        return total

    summarize(extract())


hello_airflow()
```

What this shows:
* **Import from `airflow.sdk`.** That is the Airflow 3 authoring interface. `airflow.decorators`, `airflow.models.DAG` and `airflow.datasets` are 2.x paths; see the [Task SDK quick reference](../09_reference/06_task_sdk_quick_reference.md).
* **`schedule=None`** means "only when triggered". It is also the default in 3.x. `schedule_interval` no longer exists.
* **Passing a task's return value to another task creates both the dependency and an XCom.** `summarize(extract())` makes `extract` upstream of `summarize`. The list travels through the metadata database, so keep return values small. See [XCom, Variables & Connections](../03_task_execution/04_xcom_variables_and_connections.md).
* **The last line calls the function.** A `@dag` function that is never called defines nothing, and the file parses cleanly with zero Dags in it. This is the most common "my Dag doesn't show up" cause.
* **Keep the top level cheap.** The Dag processor re-imports this file every `[dag_processor] min_file_process_interval` seconds (default `30`). Network calls or heavy imports at module level run on every parse. See [anti-patterns](../01_architecture_and_mental_model/02_anti_patterns.md).

---

## 5. See it parsed, and why it can take minutes

```bash
airflow dags list-import-errors
airflow dags list | grep hello_airflow
```

Two separate timers decide when a change appears:

| You changed | What picks it up | Default |
| :--- | :--- | :--- |
| Added a **new** file | The Dags-folder bundle refresh, `[dag_processor] refresh_interval` | `300` seconds (standalone overrides it to `5`, see §3) |
| Edited an **existing** file | Re-parse of known files, `[dag_processor] min_file_process_interval` | `30` seconds |

So outside standalone, a new file can take up to five minutes to show up, while an edit takes up to 30 seconds. To force it now, re-parse the bundle and write the result to the database:

```bash
airflow dags reserialize
```

If the Dag still doesn't appear, check `airflow dags list-import-errors` first, then the Dag processor output (component `dag-processor` in the standalone terminal, or files under `[logging] dag_processor_child_process_log_directory`, default `{AIRFLOW_HOME}/logs/dag_processor`). The [troubleshooting playbook](../06_operations_and_housekeeping/05_troubleshooting_playbook.md) has the full checklist.

---

## 6. Unpause, trigger, and read the logs

New Dags start **paused**, because `[core] dags_are_paused_at_creation` defaults to `True`. A run triggered on a paused Dag is created but stays `queued`, and nothing happens. Unpause first:

```bash
airflow dags unpause hello_airflow
airflow dags trigger hello_airflow
airflow dags list-runs hello_airflow
```

Copy the `run_id` from the last command (it looks like `manual__2026-11-23T10:15:02+00:00`) and check the task states:

```bash
airflow tasks states-for-dag-run hello_airflow '<run_id>'
```

Replace `<run_id>` with the value you copied. Task logs are files under `[logging] base_log_folder` (default `{AIRFLOW_HOME}/logs`), laid out by `[logging] log_filename_template`:

```bash
find "$AIRFLOW_HOME/logs/dag_id=hello_airflow" -name 'attempt=*.log'
```

Open the `task_id=summarize` file and you'll find `Sum of 5 numbers is 14`. In the UI, the same log is under **Dags → hello_airflow → the run → summarize → Logs**, and the `extract` task's XCom tab shows `[3, 1, 4, 1, 5]`. Each retry writes a new `attempt=N.log`, so the attempt number is in the file name. See [logging](../06_operations_and_housekeeping/04_logging.md).

> [!IMPORTANT]
> `airflow dags trigger` without `-l/--logical-date` creates a run **with no logical date** in 3.x. Tasks in that run have no `ds`, `ts`, `logical_date` or `data_interval_*` in their context, and a template that uses `{{ ds }}` fails with an undefined-variable error. See [templates & macros](../09_reference/04_templates_and_macros.md#2-the-logical-date-trap).

---

## 7. Test without the scheduler

`airflow dags test` runs one Dag run in a single process. It needs no scheduler, executor or worker, but it still uses the metadata database:

```bash
airflow dags test hello_airflow
```

It prints every task's log to your terminal, which makes it the fastest edit-run loop. Useful flags (from `cli_config.py`):

| Flag | Use |
| :--- | :--- |
| positional `logical_date` | Run for a specific date, for example `airflow dags test hello_airflow 2026-11-01`. Defaults to now, so `ds` exists here even though it doesn't for a plain `dags trigger`. |
| `-c/--conf '{"key": "value"}'` | Pass run configuration, validated against the Dag's `params`. |
| `--mark-success-pattern '<regex>'` | Mark matching tasks successful without running them (for example sensors). |
| `-f/--dagfile-path <file>` | Load the Dag from a file instead of the configured bundles. |
| `--use-executor` | Run through the configured executor instead of in-process. |

The same thing from Python is `dag.test()`; see the [Dag authoring tutorial](02_tutorial_dag_authoring.md#8-test-the-whole-pipeline-with-dagtest).

> [!TIP]
> `airflow tasks test <dag_id> <task_id>` runs a single task without checking dependencies or recording state, which is handy for a task that doesn't depend on upstream XComs.

---

## 8. Clean up

Stop standalone with `Ctrl+C`. To start over completely, remove the whole home directory (database, config, generated password, logs):

```bash
rm -rf "$AIRFLOW_HOME"
```

This deletes every run, log and setting you created. With Compose, use `docker compose down --volumes --remove-orphans`.

---

## 9. Where to go next

| You want to… | Read |
| :--- | :--- |
| Understand the components you just started | [Mental model](../01_architecture_and_mental_model/01_mental_model.md), then [anti-patterns](../01_architecture_and_mental_model/02_anti_patterns.md) |
| Build a realistic pipeline: retries, params, mapping, assets | [Tutorial: Dag authoring](02_tutorial_dag_authoring.md) |
| Practice operating it: health checks, clear, backfill, cleanup | [Tutorial: operations](03_tutorial_operations.md) |
| Choose a schedule, or trigger a Dag from data | [Scheduling, timetables & assets](../02_dag_authoring/02_scheduling_timetables_and_assets.md) |
| Test Dags properly | [Testing Dags](../02_dag_authoring/03_testing_dags.md) |
| Run something real | [Configuration](../05_deployment_and_config/01_configuration_and_tuning.md), [deployment runbook](../05_deployment_and_config/02_deployment_runbook.md), [Kubernetes & Helm](../04_infrastructure_and_cloud/03_kubernetes_and_helm_chart.md) |
| Coming from Airflow 2 | [Upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md) |
| Look up a key, command or template variable | [airflow.cfg reference](../09_reference/01_airflow_cfg_reference.md), [CLI reference](../09_reference/02_cli_reference.md), [templates & macros](../09_reference/04_templates_and_macros.md) |
