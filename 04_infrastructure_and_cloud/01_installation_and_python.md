# Installation, Python Versions and Packaging

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Installation of Airflow®](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/index.html) · [Prerequisites](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/prerequisites.html) · [Dependencies](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/dependencies.html) · [Supported versions](https://airflow.apache.org/docs/apache-airflow/3.4.0/installation/supported-versions.html)

The official installation chapter (`airflow-core/docs/installation/` in the source, "Installation of Airflow" on the website) lists every supported method. This page is the operator's version: which distributions you actually get in 3.4, why an install without constraints breaks weeks later, how to add providers without moving Airflow itself, and the first-run traps that bite in production. Every version bound below was read from the `pyproject.toml` files of the 3.4.0 source.

If you deploy with containers, read this page for the packaging model and then go to [Docker images](02_docker_images.md).

---

## 1. What "Airflow 3.4" Is Made Of

Airflow 3 is split into several Python distributions. `pip install apache-airflow` still works, but it is a meta-package that pulls in the others.

| Distribution | Version in the 3.4.0 release | What it contains | Notes |
| :--- | :--- | :--- | :--- |
| `apache-airflow` | 3.4.0 | Nothing of its own. Depends on `apache-airflow-core` at the same version, plus extras | Use this for "classic" installs. Extras on this package install providers |
| `apache-airflow-core` | 3.4.0 | Scheduler, API server, Dag processor, triggerer, CLI, UI, models, migrations | Pins `apache-airflow-task-sdk==1.4.0` exactly |
| `apache-airflow-task-sdk` | 1.4.0 | `airflow.sdk`: the Dag-authoring API and the task runner/supervisor used on workers | Declares `apache-airflow-core>=3.4.0,<3.5.0`, so a worker-only install still pulls core in 3.4 |
| `apache-airflow-providers-*` | Independent | Operators, hooks, executors (Celery, Kubernetes), secrets backends, log handlers | Released on their own schedule; see [§5](#5-installing-and-upgrading-providers) |
| `apache-airflow-ctl` | Independent | `airflowctl`, a remote CLI that talks to the REST API | Optional; see [REST API and airflowctl](../09_reference/05_rest_api_and_airflowctl.md) |

`apache-airflow-core` 3.4.0 already depends on five providers, so they are always present: `common-compat`, `common-io`, `common-sql`, `smtp` and `standard` (`BashOperator`, `PythonOperator` and the standard sensors live in `standard`).

> [!NOTE]
> Dag code imports from `airflow.sdk` (`from airflow.sdk import DAG, dag, task, Asset`). Core-side imports such as `airflow.models.DAG` are not the authoring interface in Airflow 3. See [Task SDK quick reference](../09_reference/06_task_sdk_quick_reference.md).

---

## 2. Python and OS Support

From `airflow-core/pyproject.toml` (3.4.0):

| Item | Value |
| :--- | :--- |
| `requires-python` | `>=3.11,!=3.15` |
| Python versions in the classifiers (tested) | 3.11, 3.12, 3.13, 3.14 |
| Task SDK and `airflowctl` | Same bound, `>=3.11,!=3.15` |
| Production OS | Linux only. macOS works for development; Windows only through WSL2 or containers |
| SQLite C library (dev only) | 3.15.0 or newer. Airflow checks `sqlite3.sqlite_version_info` at startup and refuses older versions |

Choosing a version:

* **New deployments:** pick the newest Python that every provider you need supports. Some providers lag behind a new Python release and mark themselves incompatible (the `!=` comment in `pyproject.toml` says so explicitly).
* **Python 3.10 and older are gone.** A 3.10 interpreter gets "no matching distribution" from pip rather than an old Airflow, provided you pin `apache-airflow==3.4.0`. If you do not pin, pip silently resolves an old 2.x release. Always pin.
* **Use the same Python minor version on every component.** The constraints file (§3) is per Python version, and Dag code parsed with one version and run with another can behave differently.

---

## 3. Constraints: the Rule You Cannot Skip

Airflow declares open-ended dependency ranges (`sqlalchemy[asyncio]>=2.0.50,!=2.1.0`, `fastapi>=0.142.0`, `pydantic>=2.12.0` and about 80 more in core alone). That is correct for a library, but it means a plain `pip install apache-airflow==3.4.0` resolves to whatever is newest on PyPI **today**. Install again next month and you get a different set, sometimes with a dependency release that breaks Airflow, a provider, or your Dags.

The project therefore publishes one **constraints file per Airflow version and per Python version**: the exact set of versions that were tested together at release time. They live in tags of the `constraints-<version>` orphan branches of the `apache/airflow` GitHub repository.

```bash
AIRFLOW_VERSION=3.4.0
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

python3 -m venv /opt/airflow-venv
. /opt/airflow-venv/bin/activate
pip install "apache-airflow[celery,postgres,cncf.kubernetes]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

| Constraints file | Use it when |
| :--- | :--- |
| `constraints-<py>.txt` | Default. Pins Airflow's dependencies **and** every provider at the version released with this Airflow |
| `constraints-no-providers-<py>.txt` | You install only core (no extras) and manage providers yourself |
| Your own file (`pip freeze > my-constraints.txt`) | After you have added providers or libraries and tested the result. Commit it and install from it everywhere |

Rules that follow from how pip works:

1. **Constraints only apply to the one `pip install` command they are passed to.** A later `pip install some-lib` without them can upgrade or downgrade anything, including Airflow.
2. **Constraints are not requirements.** They pin a package only if something asks for it; they never install anything by themselves.
3. **A constraints file pins providers to the release-time versions.** If you need a newer provider than the one released with 3.4.0, install it in a separate command (§5).
4. **Treat a new Airflow patch release as the way to get new dependency versions.** Each release re-tests and re-pins the whole set.

> [!WARNING]
> Only `pip` and `uv` are supported installers. Poetry and pip-tools do not have pip's constraint semantics; if you use them you must convert the constraints file into their own lock format yourself, and the project does not support that path.

---

## 4. Extras

Extras on `apache-airflow` do two different things:

| Kind | Examples | Effect |
| :--- | :--- | :--- |
| Core extras (defined on `apache-airflow-core`) | `async`, `graphviz`, `gunicorn`, `kerberos`, `memray`, `otel`, `statsd` | Add Python libraries to core. Install no provider |
| Core group extra | `all` on `apache-airflow-core`, `all-core` on `apache-airflow` | `graphviz,gunicorn,kerberos,otel,statsd` together |
| Provider extras | `postgres`, `mysql`, `celery`, `cncf.kubernetes`, `amazon`, `google`, `microsoft.azure`, `fab`, `git`, ... | Install `apache-airflow-providers-<name>` |
| Meta-package extras | `aiobotocore`, `pandas`, `polars`, `ldap`, `google-auth`, `cloudpickle`, ... | Install supporting libraries that some providers use |
| `all` on `apache-airflow` | | Every core extra **and every provider**. Hundreds of packages. Never use it for production |

What you normally need, by role:

| You run | Minimum extras |
| :--- | :--- |
| Postgres metadata DB | `postgres` (brings the `psycopg` driver; see §6) |
| MySQL metadata DB | `mysql` (brings `mysqlclient`, which needs MySQL client libraries and a compiler to build) |
| CeleryExecutor | `celery`, plus a broker client such as `redis` |
| KubernetesExecutor or `KubernetesPodOperator` | `cncf.kubernetes` |
| FAB auth manager (users, roles, LDAP/OAuth login) | `fab` |
| Dag bundles from Git | `git` |
| StatsD or OpenTelemetry metrics | `statsd` / `otel` |

---

## 5. Installing and Upgrading Providers

Providers are versioned independently of core. Each one declares a minimum Airflow version (for example `apache-airflow>=2.11.0` in the current Postgres provider), so a provider released after 3.4.0 normally still installs on 3.4.

The safe pattern is two commands: Airflow with constraints, then providers **without** constraints but with Airflow pinned in the same command so the resolver can't move it:

```bash
pip install "apache-airflow==3.4.0" --constraint "${CONSTRAINT_URL}"
pip install "apache-airflow==3.4.0" "apache-airflow-providers-amazon==<provider_version>"
```

Replace `<provider_version>` with the provider release you tested. If the second command would need to change Airflow, it fails with a resolver error instead of silently replacing core. That error is what you want.

After any provider change:

```bash
pip check                       # broken or conflicting requirements
airflow providers list          # what Airflow actually discovered, with versions
airflow info                    # Python, platform, paths, providers, DB URL (password masked)
```

> [!TIP]
> Freeze the environment that passed your tests (`pip freeze > constraints-prod.txt`) and install production from that file with `--constraint`. That is the only way to rebuild exactly the same environment later, for example during [disaster recovery](../07_disaster_recovery/01_backup_and_disaster_recovery.md).

If two providers need incompatible versions of a library, no single environment can hold both. Run the conflicting tasks in their own environment instead: a separate image with `KubernetesPodOperator`, `@task.virtualenv` / `@task.external_python`, or a dedicated Celery queue with its own workers. See [Dag design patterns](../02_dag_authoring/01_dag_design_patterns.md).

---

## 6. The Postgres Driver Changed in 3.4

Airflow 3.4.0 makes **psycopg 3** the default synchronous and asynchronous driver for a Postgres metadata database (`airflow-core/src/airflow/configuration.py`, `_upgrade_postgres_metastore_conn`):

| `[database] sql_alchemy_conn` scheme | What 3.4 does |
| :--- | :--- |
| `postgresql+psycopg://` | Used as is |
| `postgresql://` (no driver) | Rewritten to `postgresql+psycopg://` if `psycopg` is importable, else `postgresql+psycopg2://`. No warning |
| `postgres://` or `postgres+psycopg2://` | Rewritten the same way, with a `FutureWarning`: SQLAlchemy does not accept these schemes |
| `postgresql+psycopg2://` | Never rewritten. Needs `psycopg2` installed (`apache-airflow-providers-postgres[psycopg2]`) |

Core does not ship a Postgres driver; it comes with the `postgres` provider. With psycopg 3, Airflow also defaults `prepare_threshold` to `None` (no server-side prepared statements) so that it works behind PgBouncer in transaction mode. Details are in [Metadata database](04_metadata_database.md#3-connection-url-and-driver).

---

## 7. Installing with uv

`uv pip` follows pip's constraint semantics and is much faster. The same rules apply.

```bash
uv venv --python 3.12 /opt/airflow-venv
. /opt/airflow-venv/bin/activate
uv pip install "apache-airflow[celery,postgres]==3.4.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt"
```

For a throwaway local instance, `uvx apache-airflow standalone` or `uv tool install "apache-airflow==3.4.0"` work, but they do not use constraints. Use them for trying things out only.

---

## 8. OS-Level Dependencies

Wheels exist for most dependencies on Linux x86-64 and arm64, so a plain install usually needs only Python. You need system packages when:

| Situation | What to install (Debian package names) |
| :--- | :--- |
| `mysql` extra (`mysqlclient` builds from source) | A C compiler, `pkg-config`, the MySQL/MariaDB client development headers *(exact package names not verified against source)* |
| `kerberos` extra | `krb5-user` and the Kerberos development headers |
| `odbc` extra / MSSQL hooks | `unixodbc` and the vendor ODBC driver |
| `graphviz` extra | The `graphviz` system package (skipped on macOS by the extra itself) |
| SQLite older than 3.15 on the host | A newer SQLite library; check with `python3 -c 'import sqlite3; print(sqlite3.sqlite_version)'` |

The official docs list the full set used in CI (`airflow-core/docs/installation/dependencies.rst`). The published Docker images already contain the database clients and runtime libraries; building on them is usually easier than maintaining a host install (see [Docker images](02_docker_images.md)).

---

## 9. First-Run Traps

1. **`AIRFLOW_HOME` defaults to `~/airflow`.** Set it explicitly for every component and every user that runs `airflow` commands, or they read different configs.
2. **The first command writes `airflow.cfg` with freshly generated secrets.** If no config file exists, Airflow generates a random `[core] fernet_key` and `[api_auth] jwt_secret` and writes them into the new `airflow.cfg` (`write_default_airflow_configuration_if_needed` in `configuration.py`). On a second machine, or in a fresh container, a *different* key is generated. Result: encrypted connections become unreadable and task JWTs fail validation. Set `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__API__SECRET_KEY` explicitly and identically on every component. See [Security hardening](../05_deployment_and_config/03_security_hardening.md).
3. **The default database is SQLite** (`sqlite:///{AIRFLOW_HOME}/airflow.db`) and the default executor is `LocalExecutor`. Fine on a laptop; not for anything shared. See [Metadata database](04_metadata_database.md#1-supported-backends).
4. **The 2.x `init` subcommand of `airflow db` is gone.** Create or upgrade the schema with `airflow db migrate`. Components started against an unmigrated database exit with "Database migration required" when they have no TTY.
5. **The default auth manager in core is `SimpleAuthManager`** with `[core] simple_auth_manager_users = admin:admin`. Its generated passwords go into `AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`. Use the FAB auth manager (`fab` extra) or another real auth manager in production.
6. **`airflow standalone` is a development tool.** It starts every component in one process tree with SQLite and generated credentials.

---

## 10. Minimal Bare-Metal Layout

A VM-based install that survives reboots and upgrades:

```bash
# One virtualenv per Airflow version, so an upgrade is a new directory, not an in-place change
python3.12 -m venv /opt/airflow/venv-3.4.0
/opt/airflow/venv-3.4.0/bin/pip install "apache-airflow[celery,postgres,statsd]==3.4.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt"
ln -sfn /opt/airflow/venv-3.4.0 /opt/airflow/venv

export AIRFLOW_HOME=/etc/airflow           # airflow.cfg lives here, owned by the service user
/opt/airflow/venv/bin/airflow config get-value database sql_alchemy_conn
/opt/airflow/venv/bin/airflow db check
/opt/airflow/venv/bin/airflow db migrate
```

Then run each component (`airflow scheduler`, `airflow api-server`, `airflow dag-processor`, `airflow triggerer`, and `airflow celery worker` if you use Celery) under your service manager. Keeping the previous `venv-<version>` directory is your binary rollback; the database schema is a separate question covered in [Metadata database §6](04_metadata_database.md#6-schema-migrations).

---

## See Also

* [Docker images](02_docker_images.md): the same packaging rules applied to container builds.
* [Kubernetes and the Helm chart](03_kubernetes_and_helm_chart.md)
* [Metadata database](04_metadata_database.md)
* [Upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md)
* [Configuration and tuning](../05_deployment_and_config/01_configuration_and_tuning.md)
