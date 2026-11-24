# Glossary

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Architecture Overview](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/overview.html) · [Public Interface for Airflow 3.0+](https://airflow.apache.org/docs/apache-airflow/3.4.0/public-airflow-interface.html)

Short definitions of the terms this guide uses, as they apply to Airflow 3.4. Each entry links to the section that explains it in full. Where Airflow 2 used a different name or meaning, the entry says so, because carrying the 2.x meaning over is a common cause of wrong answers.

---

## 1. Components

| Term | Meaning | More |
| :--- | :--- | :--- |
| **API server** | The `airflow api-server` process. It serves the UI, the public REST API (`/api/v2`) and the Execution API (`/execution/`). It replaces the Airflow 2 webserver | [Mental model §1](../01_architecture_and_mental_model/01_mental_model.md#1-components-and-their-boundaries) |
| **Dag processor** | The `airflow dag-processor` process. It imports Dag files from each bundle in subprocesses, serializes them and writes them to the database. It also runs Dag-level callbacks. In Airflow 3 it is a separate, required component | [Mental model §1](../01_architecture_and_mental_model/01_mental_model.md#1-components-and-their-boundaries) |
| **Execution API** | The HTTP API under `/execution/` on the API server. Workers use it, with a short-lived JWT, for everything that needs the database: state changes, heartbeats, Variables, Connections, XComs | [Mental model §1](../01_architecture_and_mental_model/01_mental_model.md#what-the-boundaries-mean-in-practice) |
| **Executor** | The class inside the scheduler process that takes workloads and starts them somewhere: `LocalExecutor` (core), or the Celery, Kubernetes and Edge executors from providers. Several can run at once | [Executors §1](../03_task_execution/01_executors.md#1-what-an-executor-does-in-airflow-3) |
| **Metadata database** | The PostgreSQL or MySQL database (SQLite for local testing only) that holds all Airflow state | [Metadata database](../04_infrastructure_and_cloud/04_metadata_database.md) |
| **Scheduler** | The `airflow scheduler` process. It creates Dag runs, decides which task instances are ready, and hands them to the executor. In Airflow 3 it never imports Dag files; it reads serialized Dags | [Mental model §1](../01_architecture_and_mental_model/01_mental_model.md#1-components-and-their-boundaries) |
| **Supervisor** | The worker-side process (`supervise_task` in `task-sdk/src/airflow/sdk/execution_time/supervisor.py`) that starts your task as a child process, relays its requests to the Execution API, sends heartbeats and enforces timeouts | [Executors §1](../03_task_execution/01_executors.md#1-what-an-executor-does-in-airflow-3) |
| **Triggerer** | The `airflow triggerer` process. It runs async triggers for deferred tasks and asset watchers | [Deferrable operators §7](../03_task_execution/02_deferrable_operators_and_triggers.md#7-running-the-triggerer) |
| **Worker** | Whatever process runs task code: a child of the scheduler (Local), a Celery worker, a Kubernetes pod or an Edge worker. Workers never connect to the metadata database | [Executors](../03_task_execution/01_executors.md) |
| **Workload** | The unit an executor or triggerer receives, defined in `airflow-core/src/airflow/executors/workloads/`: `ExecuteTask`, `ExecuteCallback`, `TestConnection`, `RunTrigger` | [Executors §1](../03_task_execution/01_executors.md#1-what-an-executor-does-in-airflow-3) |

---

## 2. Dags and their code

| Term | Meaning | More |
| :--- | :--- | :--- |
| **Dag** | A workflow: tasks plus their dependencies and schedule. Written **Dag** in prose; `DAG` is the Python class (`from airflow.sdk import DAG`) | [Dag design patterns](../02_dag_authoring/01_dag_design_patterns.md) |
| **Dag bundle** | A named source of Dag files: a local folder (the default `dags-folder` bundle), a Git repository, or object storage. Versioned bundles let a run keep executing the code it started with | [Mental model §3](../01_architecture_and_mental_model/01_mental_model.md#3-dag-bundles) |
| **Dag version** | A row in `dag_version`. A new one is created when a Dag's serialized structure changes and the current version already has task instances | [Mental model §2](../01_architecture_and_mental_model/01_mental_model.md#2-serialized-dags-and-dag-versions) |
| **Draining** | New in 3.4: a Dag scheduling state between `active` and `paused`. No new scheduled or asset-triggered runs are created; queued and running ones finish, then the Dag is paused | [Mental model §4](../01_architecture_and_mental_model/01_mental_model.md#4-life-of-a-dag-run) |
| **Import error** | An exception raised while importing a Dag file. The UI lists it in the import-errors banner until the file imports cleanly. List them with `airflow dags list-import-errors` | [Troubleshooting §1](../06_operations_and_housekeeping/05_troubleshooting_playbook.md#1-dag-missing-import-errors-parse-timeouts) |
| **Provider** | A separately released package (`apache-airflow-providers-*`) with operators, hooks, executors, bundles or secrets backends for one system. Provider versions are independent of core | [Installation §5](../04_infrastructure_and_cloud/01_installation_and_python.md#5-installing-and-upgrading-providers) |
| **Serialized Dag** | The JSON form of a parsed Dag, stored in `serialized_dag`. The scheduler, UI and REST API work only from it | [Mental model §2](../01_architecture_and_mental_model/01_mental_model.md#2-serialized-dags-and-dag-versions) |
| **Task SDK** | The `apache-airflow-task-sdk` package and its `airflow.sdk` namespace: the public interface for writing Dags in Airflow 3. Replaces imports from `airflow.models`, `airflow.decorators` and `airflow.datasets` | [Task SDK quick reference](06_task_sdk_quick_reference.md) |
| **TaskFlow** | Writing tasks as decorated Python functions (`@task`), with return values passed as XComs | [Dag design patterns §1](../02_dag_authoring/01_dag_design_patterns.md#1-taskflow-or-classic-operators) |

---

## 3. Runs, tasks and dates

| Term | Meaning | More |
| :--- | :--- | :--- |
| **Dag run** | One execution of a Dag, a row in `dag_run`. States: `queued`, `running`, `success`, `failed` | [Mental model §4](../01_architecture_and_mental_model/01_mental_model.md#4-life-of-a-dag-run) |
| **Data interval** | The window of data a run should process (`data_interval_start` / `data_interval_end`). Zero length under the 3.x default `CronTriggerTimetable`; `None` when the run has no logical date | [Mental model §6](../01_architecture_and_mental_model/01_mental_model.md#6-dates-on-a-dag-run-logical_date-data-interval-run_after) |
| **Logical date** | The date a run "represents". Unique per Dag when set, and **nullable**: manual and asset-triggered runs can have none. Replaces Airflow 2's `execution_date` | [Mental model §6](../01_architecture_and_mental_model/01_mental_model.md#6-dates-on-a-dag-run-logical_date-data-interval-run_after) |
| **Map index** | The position of one task instance created by dynamic task mapping (`.expand()`). `-1` means the task is not mapped | [Dag design patterns §8](../02_dag_authoring/01_dag_design_patterns.md#8-dynamic-task-mapping) |
| **`run_after`** | The earliest time a run may start. Always set; the UI shows it as the run's date | [Mental model §6](../01_architecture_and_mental_model/01_mental_model.md#6-dates-on-a-dag-run-logical_date-data-interval-run_after) |
| **`run_id`** | A run's identifier, unique per Dag. Generated as `<run_type>__<run_after ISO timestamp>`, with a random suffix added when the run has no logical date (`DagRun.generate_run_id` in `airflow-core/src/airflow/models/dagrun.py`) | [CLI guide §4](../06_operations_and_housekeeping/01_cli_operations_guide.md#4-runs-and-tasks-trigger-pause-clear) |
| **Run type** | How a run was created (`DagRunType`): `scheduled`, `manual`, `backfill`, `operator_triggered`, `asset_triggered`, `asset_materialization` | [Mental model §4](../01_architecture_and_mental_model/01_mental_model.md#4-life-of-a-dag-run) |
| **Task instance** | One task in one Dag run (and one map index), a row in `task_instance`. Its state moves from `None` through `scheduled`, `queued`, `running` to a terminal state | [Mental model §5](../01_architecture_and_mental_model/01_mental_model.md#5-life-of-a-task-instance) |
| **Try number** | Which attempt of a task instance this is (`ti.try_number`). Retries and clears start a new try; each try gets its own log file | [Templates and macros §3](04_templates_and_macros.md#3-variables-present-in-every-task-run) |

---

## 4. Scheduling

| Term | Meaning | More |
| :--- | :--- | :--- |
| **Asset** | A named piece of data (`Asset("s3://…")`) that tasks declare as `outlets` and Dags can be scheduled on. Called a *Dataset* in Airflow 2 | [Scheduling §5](../02_dag_authoring/02_scheduling_timetables_and_assets.md#5-assets) |
| **Asset alias** | An `AssetAlias("name")` placed in `outlets` when the concrete asset is only known at run time. Consumers can schedule on the alias | [Scheduling §5](../02_dag_authoring/02_scheduling_timetables_and_assets.md#5-assets) |
| **Asset event** | A record (row in `asset_event`) that an asset was updated, written when a producing task succeeds, through the API, or by an asset watcher. Asset events are what trigger consuming Dags | [Scheduling §5](../02_dag_authoring/02_scheduling_timetables_and_assets.md#5-assets) |
| **Asset watcher** | An `AssetWatcher` that attaches a trigger to an asset, so an external event (a queue message, for example) creates an asset event. Runs in the triggerer | [Scheduling §8](../02_dag_authoring/02_scheduling_timetables_and_assets.md#8-event-driven-scheduling-with-asset-watchers) |
| **Backfill** | Runs for a past date range. In 3.x a backfill is a database object that the CLI or REST API creates; the scheduler then creates and runs the Dag runs. The CLI returns immediately instead of running tasks itself as in 2.x | [Scheduling §4](../02_dag_authoring/02_scheduling_timetables_and_assets.md#4-backfills-in-3x) |
| **Catchup** | Whether the scheduler creates runs for intervals missed since `start_date`. Defaults to `False` in 3.x (`[scheduler] catchup_by_default`) | [Scheduling §3](../02_dag_authoring/02_scheduling_timetables_and_assets.md#3-start_date-end_date-and-catchup) |
| **Partitioned Dag** | A Dag whose runs carry a `partition_key` (3.2+), so each run processes one partition of an asset | [Scheduling §7](../02_dag_authoring/02_scheduling_timetables_and_assets.md#7-partitioned-dags) |
| **Timetable** | The object that turns a `schedule` argument into run dates. A cron string becomes a `CronTriggerTimetable` in 3.x | [Scheduling §2](../02_dag_authoring/02_scheduling_timetables_and_assets.md#2-trigger-timetables-vs-data-interval-timetables) |

---

## 5. Execution

| Term | Meaning | More |
| :--- | :--- | :--- |
| **Deferral** | A running task hands its wait to a trigger (it raises `TaskDeferred`) and frees its worker slot. The task instance goes to `deferred` until the trigger fires | [Deferrable operators §2](../03_task_execution/02_deferrable_operators_and_triggers.md#2-the-lifecycle-in-34) |
| **Heartbeat timeout (zombie)** | A running task whose supervisor stopped heartbeating for `[scheduler] task_instance_heartbeat_timeout` (300 s). The scheduler fails or retries it. Airflow 2 called these zombies | [Troubleshooting §4](../06_operations_and_housekeeping/05_troubleshooting_playbook.md#4-task-failed-without-a-heartbeat-zombies) |
| **Pool** | A named set of slots that limits how many task instances run at once across Dags. Every task uses `default_pool` unless told otherwise | [Concurrency §3](../03_task_execution/03_concurrency_pools_and_priority.md#3-pools) |
| **Priority weight** | The number the scheduler uses to order ready task instances when slots are short, computed by the task's `weight_rule` | [Concurrency §5](../03_task_execution/03_concurrency_pools_and_priority.md#5-priority) |
| **Trigger** | A small async Python class that waits for an event and fires it. Triggers run in the triggerer | [Deferrable operators §3](../03_task_execution/02_deferrable_operators_and_triggers.md#3-writing-a-deferrable-operator-and-trigger) |
| **XCom** | A small value passed between tasks, stored through the Execution API. Pass references (paths, ids), not data | [XCom, Variables and Connections §2](../03_task_execution/04_xcom_variables_and_connections.md#2-xcom-essentials-and-size-limits) |

---

## 6. Operations and security

| Term | Meaning | More |
| :--- | :--- | :--- |
| **Auth manager** | The pluggable component that handles UI and REST API login and permissions. The default is the Simple auth manager, meant for development | [Security hardening §3](../05_deployment_and_config/03_security_hardening.md#3-authentication-choose-an-auth-manager) |
| **Constraints file** | The pinned dependency list published for each Airflow and Python version. Install with it, or pip may pick incompatible versions | [Installation §3](../04_infrastructure_and_cloud/01_installation_and_python.md#3-constraints-the-rule-you-cannot-skip) |
| **`db clean`** | `airflow db clean`: deletes old rows from metadata tables. Irreversible without a backup; always run with `--dry-run` first | [Housekeeping §2](../06_operations_and_housekeeping/02_housekeeping_and_automations.md#2-airflow-db-clean-what-it-actually-does) |
| **Fernet key** | `[core] fernet_key`, which encrypts Connection passwords and Variables in the database. Losing it makes those values unreadable | [Housekeeping §7](../06_operations_and_housekeeping/02_housekeeping_and_automations.md#7-rotating-the-fernet-key) |
| **Secrets backend** | An external store (Vault, a cloud secrets manager) that Airflow reads Variables and Connections from, set with `[secrets] backend` | [XCom, Variables and Connections §6](../03_task_execution/04_xcom_variables_and_connections.md#6-secrets-backends) |
