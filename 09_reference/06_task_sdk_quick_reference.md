# Task SDK Quick Reference (`airflow.sdk`)

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

The official API reference lists the Task SDK alphabetically, one class per page. This page puts **every public name** in `airflow.sdk.__all__` for Airflow 3.4 (Task SDK `__version__` `1.4.0`, file `task-sdk/src/airflow/sdk/__init__.py` on `v3-4-stable`) on one page, grouped by what you use it for. Each entry has a one-line description and, where there is one, the Airflow 2 import it replaces. Use it to port Dags, and to check whether a name you have seen elsewhere really is public.

How to read the "Airflow 2 path" column:
* **†** marks rows listed in the official migration table (`airflow-core/docs/installation/upgrading_to_airflow3.rst`).
* Rows without † come from Airflow 2.x knowledge *(not verified against the 3.4 source)*.
* "new" means there is no 2.x equivalent.
* Many old paths still import in 3.4 through deprecation shims (for example `airflow.models.__init__` and `airflow.utils.__init__` redirect to `airflow.sdk`), but they warn and will be removed. Don't use them in new code.

```python
from airflow.sdk import DAG, Asset, Variable, dag, task  # every name below imports this way
```

---

## 1. Dags, Tasks and Structure

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `DAG` | The Dag class (context manager or plain object) | `airflow.models.dag.DAG` † |
| `dag` | Decorator that turns a function into a Dag factory | `airflow.decorators.dag` † |
| `task` | Decorator for Python tasks; also `@task.virtualenv`, `@task.docker`, etc. from providers | `airflow.decorators.task` † |
| `task_group` | Decorator that builds a `TaskGroup` from a function; in 3.4 also `.loop(max_iterations=..., until=...)` to repeat its body | `airflow.decorators.task_group` † |
| `TaskGroup` | Groups tasks visually and by `group_id` prefix | `airflow.utils.task_group.TaskGroup` † |
| `setup` / `teardown` | Mark a `@task` as setup or teardown work | `airflow.decorators.setup` / `teardown` † |
| `result` | Decorator placed above `@task` to mark that task's return value as the Dag's result | new |
| `LoopDecision` | Return type for a loop's `until` callable: `stop` and an optional `reason` (max 512 bytes) | new |
| `chain` | Set dependencies along a sequence, pairing lists element by element | `airflow.models.baseoperator.chain` |
| `chain_linear` | Like `chain`, but every element of one list depends on every element of the previous one | `airflow.models.baseoperator.chain_linear` |
| `cross_downstream` | Make every task in one list upstream of every task in another | `airflow.models.baseoperator.cross_downstream` |
| `Label` | Label for a dependency edge in the graph view (`a >> Label("ok") >> b`) | `airflow.utils.edgemodifier.Label` |
| `EdgeModifier` | Base class behind `Label` | `airflow.utils.edgemodifier.EdgeModifier` |
| `TriggerRule` | Enum of trigger rules (`all_success`, `none_failed`, …) | `airflow.utils.trigger_rule.TriggerRule` |
| `WeightRule` | Enum for `weight_rule` (`downstream`, `upstream`, `absolute`) | `airflow.utils.weight_rule.WeightRule` |
| `Param` | A typed, validated Dag or run parameter | `airflow.models.param.Param` † |
| `ParamsDict` | The dict of `Param`s on a Dag or task | `airflow.models.param.ParamsDict` † |
| `literal` | Wrap a template-field value so Jinja does **not** render it | `airflow.utils.template.literal` |

## 2. Operators, Sensors, Hooks and Mixins

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `BaseOperator` | Base class for operators; `defer()`, `executor=`, `pool=`, `retry_policy=` and other task parameters live here | `airflow.models.baseoperator.BaseOperator` † |
| `BaseAsyncOperator` | Base for operators with an `async aexecute()` that runs **on the worker** (not deferral) | new |
| `BaseSensorOperator` | Base class for sensors (`poke`, `mode`, `timeout`, deferrable support) | `airflow.sensors.base.BaseSensorOperator` † |
| `PokeReturnValue` | Return from `poke()` to signal done and push an XCom at once | `airflow.sensors.base.PokeReturnValue` |
| `BaseBranchOperator` | Base for operators that choose which downstream tasks run | `airflow.operators.branch.BaseBranchOperator` |
| `BranchMixIn` | Mixin with the branching skip logic | `airflow.operators.branch.BranchMixIn` |
| `SkipMixin` | Mixin to skip downstream tasks | `airflow.models.skipmixin.SkipMixin` |
| `ResumableJobMixin` | For operators that submit one external job and poll it. Stores the job ID in the task state store, so a retry after a worker crash reconnects instead of resubmitting | new |
| `BaseHook` | Base class for hooks; `BaseHook.get_connection(conn_id)` | `airflow.hooks.base.BaseHook` † |
| `BaseOperatorLink` | Extra link shown on a task in the UI | `airflow.models.baseoperatorlink.BaseOperatorLink` † |
| `BaseNotifier` | Base for notifiers used as callbacks (`on_failure_callback=MyNotifier(...)`) | `airflow.notifications.basenotifier.BaseNotifier` † |

`BaseTrigger`, `TriggerEvent` and `StartTriggerArgs` are **not** in `airflow.sdk`; import them from `airflow.triggers.base`. See [Deferrable operators](../03_task_execution/02_deferrable_operators_and_triggers.md#3-writing-a-deferrable-operator-and-trigger).

## 3. Runtime: Context, Data and Configuration

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `Context` | `TypedDict` of the task context (`ti`, `dag_run`, `params`, `logical_date`, …) | `airflow.utils.context.Context` † |
| `get_current_context` | Return the running task's `Context` from inside a task | `airflow.operators.python.get_current_context` |
| `get_parsing_context` | Tells Dag code which `dag_id`/`task_id` is being parsed, to skip unneeded work | `airflow.utils.dag_parsing_context.get_parsing_context` |
| `TaskInstance` | Protocol type of the runtime task instance (`context["ti"]`); use it for type hints, not to query the DB | `airflow.models.taskinstance.TaskInstance` (the ORM model, no longer usable from tasks) |
| `TaskInstanceState` / `DagRunState` | State enums | `airflow.utils.state.TaskInstanceState` / `DagRunState` |
| `Variable` | `Variable.get/set/delete/keys`, through the Execution API | `airflow.models.variable.Variable` † |
| `Connection` | `Connection.get(conn_id)`, `from_json`, `get_uri`, `extra_dejson` | `airflow.models.connection.Connection` † |
| `BaseXCom` | Base class for custom XCom backends (`store_value` / `load_value` in 3.4) | `airflow.models.xcom.BaseXCom` |
| `XComArg` | Reference to a task's output (what `@task` calls return) | `airflow.models.xcom_arg.XComArg` |
| `ObjectStoragePath` | `pathlib`-style access to S3, GCS, Azure and others, using Airflow connections | `airflow.io.path.ObjectStoragePath` † (`airflow.io.*`) |
| `NEVER_EXPIRE` | `retention=` value for `context["task_state_store"].set(...)`: the key never expires | new |
| `conf` | The SDK's config parser (`conf.get("core", "...")`) | `airflow.configuration.conf` |
| `macros` | Module of template macros (`macros.ds_add`, …) | `airflow.macros` |
| `lineage` | Module for hook-level lineage collection | `airflow.lineage.hook` |

Details: [XCom, Variables and Connections](../03_task_execution/04_xcom_variables_and_connections.md), [Templates and macros](04_templates_and_macros.md).

## 4. Assets (formerly Datasets)

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `Asset` | A data asset with `name` and `uri`; used in `outlets`, `inlets` and `schedule` | `airflow.datasets.Dataset` † |
| `AssetAlias` | Name resolved at runtime to concrete assets | `airflow.datasets.DatasetAlias` † |
| `AssetAll` / `AssetAny` | AND / OR conditions over assets (also via `&` and `\|`) | `airflow.datasets.DatasetAll` / `DatasetAny` † |
| `asset` | Decorator that defines an asset and the one-task Dag that produces it | new |
| `Metadata` | Yield from a task to attach `extra` to an asset event | `airflow.datasets.metadata.Metadata` |
| `AssetWatcher` | Binds a `BaseEventTrigger` to an asset for event-driven scheduling (runs in the triggerer) | new |
| `AssetAccessControl` | Multi-team control of which teams may produce or consume an asset | new |

## 5. Timetables and Schedules

Pass any of these as `schedule=`. Cron strings, `timedelta`, presets like `"@daily"` and assets are also accepted directly.

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `CronTriggerTimetable` | Run at each cron tick; the run covers no data interval (default for cron strings in 3.x) | `airflow.timetables.trigger.CronTriggerTimetable` |
| `CronDataIntervalTimetable` | 2.x-style cron with data intervals: the run starts at the **end** of its interval | `airflow.timetables.interval.CronDataIntervalTimetable` |
| `DeltaTriggerTimetable` | Run every `timedelta`, no data interval | `airflow.timetables.trigger.DeltaTriggerTimetable` |
| `DeltaDataIntervalTimetable` | Run every `timedelta` with data intervals | `airflow.timetables.interval.DeltaDataIntervalTimetable` |
| `MultipleCronTriggerTimetable` | Several cron expressions in one schedule | new |
| `EventsTimetable` | Run at an explicit list of datetimes | `airflow.timetables.events.EventsTimetable` |
| `AssetTriggeredTimetable` | Run when an asset condition is met (what `schedule=[Asset(...)]` builds) | `airflow.timetables.simple.DatasetTriggeredTimetable` |
| `AssetOrTimeSchedule` | Run on a time schedule **or** when assets update | `airflow.timetables.datasets.DatasetOrTimeSchedule` |
| `AssetAndTimeSchedule` | Time schedule that only fires when the asset condition is also met | new |
| `CronPartitionTimetable` | Cron schedule whose runs carry partition keys | new |
| `PartitionedAssetTimetable` | Asset-triggered schedule for partitioned assets, with per-asset `PartitionMapper`s | new |
| `PartitionedAtRuntime` | Marker: partition keys are decided at runtime | new |

See [Scheduling, timetables and assets](../02_dag_authoring/02_scheduling_timetables_and_assets.md).

## 6. Partition Mapping (all new in 3.x)

Used with `PartitionedAssetTimetable` to map upstream partition keys to downstream Dag run partitions.

| Name | What it does |
| :--- | :--- |
| `PartitionMapper` | Base class: maps asset-event keys to target partitions |
| `IdentityMapper` | Key unchanged (the default) |
| `AllowedKeyMapper` | Accepts only keys in an allowed set |
| `FixedKeyMapper` | Collapses every upstream key onto one fixed key (pair with `SegmentWindow`) |
| `ChainMapper` | Applies several mappers in sequence |
| `ProductMapper` | Combines mappers into a multi-dimensional key |
| `StartOfHourMapper`, `StartOfDayMapper`, `StartOfWeekMapper`, `StartOfMonthMapper`, `StartOfQuarterMapper`, `StartOfYearMapper` | Normalize a time key to the start of its hour, day, ISO week (Monday), month, quarter or year |
| `RollupMapper` | Many upstream keys → one downstream key, using an `upstream_mapper`, a `Window` and a wait policy |
| `FanOutMapper` | One coarse upstream key → many downstream keys over a `Window` |
| `Window` | Base: which upstream keys make up one downstream key |
| `HourWindow` / `DayWindow` / `WeekWindow` / `MonthWindow` / `QuarterWindow` / `YearWindow` | 60 minute keys / 24 hourly keys / 7 daily keys / the month's daily keys / 3 monthly keys / 12 monthly keys |
| `SegmentWindow` | A fixed categorical set of keys forming one period |
| `WaitForAll` | Fire only when every expected upstream key has arrived |
| `MinimumCount` | Fire when at least `n` keys arrived (negative `n`: all but `abs(n)`) |

## 7. Retries, Deadlines and Callbacks

| Name | What it is | Airflow 2 path |
| :--- | :--- | :--- |
| `RetryPolicy` | Base class for `retry_policy=`. Its `evaluate()` runs in the worker with the exception object | new |
| `ExceptionRetryPolicy` | Ordered `RetryRule`s mapping exception types to an action; first match wins | new |
| `RetryRule` | One mapping: exception class(es) or import path(s) → action | new |
| `RetryAction` | Enum of outcomes (`RETRY`, …). A policy can fail a task early but cannot retry past `retries` | new |
| `RetryDecision` | Result of evaluating a policy | new |
| `ChainRetryPolicy` | Asks several policies in order; the first non-`DEFAULT` decision wins | new |
| `DeadlineAlert` | Dag-level deadline: a reference time plus an interval, and a callback when missed | replaces 2.x SLAs (`sla=`, `sla_miss_callback`) |
| `DeadlineReference` | Built-in reference points (for example Dag run queued or logical time, fixed datetime) | new |
| `BaseDeadlineReference` | Base class for a custom reference point | new |
| `deadline_reference` | Decorator registering a custom reference (also list it in a plugin's `deadline_references`) | new |
| `AsyncCallback` | Deadline callback run in the **triggerer** (top-level async callable) | new |
| `SyncCallback` | Deadline callback run by an **executor** on a worker | new |

---

## 8. Not in `airflow.sdk.__all__`, but Often Needed

| You want | Import |
| :--- | :--- |
| Skip or fail a task explicitly | `from airflow.sdk.exceptions import AirflowSkipException, AirflowFailException` |
| Deferral exceptions | `from airflow.sdk.exceptions import TaskDeferred, TaskDeferralTimeout, TaskDeferralError` |
| Mask a value in logs | `from airflow.sdk.log import mask_secret` |
| Triggers | `from airflow.triggers.base import BaseTrigger, TriggerEvent, StartTriggerArgs, TaskSuccessEvent` |
| Timezone helpers | `from airflow.sdk import timezone` (submodule, e.g. `timezone.utcnow()`) |
| Standard operators (`BashOperator`, `PythonOperator`, `EmptyOperator`, `TimeSensor`, …) | `airflow.providers.standard.*` (the `apache-airflow-providers-standard` package) |

> [!WARNING]
> The type stub `task-sdk/src/airflow/sdk/__init__.pyi` in 3.4 disagrees with the runtime `__all__`. It lists `WaitPolicy` and `SecretCache`, and leaves out `BaseDeadlineReference`, `deadline_reference`, `lineage`, `ParamsDict`, `result` and `TaskInstance`. `from airflow.sdk import WaitPolicy` **fails at runtime** (there is no lazy import for it). `SecretCache` imports at runtime but is internal. Trust the runtime list on this page over IDE autocompletion.

> [!NOTE]
> Gone in 3.x, with no `airflow.sdk` equivalent: `SubDagOperator`; `execution_date` (use `logical_date`, or `dag_run.run_after` for runs with no logical date); `schedule_interval` and `timetable=` (use `schedule=`); task-level `sla` (still accepted by `BaseOperator`, but it only emits a warning and does nothing; use `DeadlineAlert`); `provide_session` and ORM access from task code (use the Execution API objects above). See [Upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md).

---

**See also:** [Dag design patterns](../02_dag_authoring/01_dag_design_patterns.md), [Testing Dags](../02_dag_authoring/03_testing_dags.md), and [Executors](../03_task_execution/01_executors.md) for `executor=`.
