# Templates & Macros Reference (Airflow 3.4)

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Templates reference](https://airflow.apache.org/docs/apache-airflow/3.4.0/templates-ref.html)

This page lists every Jinja template variable, filter and macro a task sees in Airflow 3.4, with its type, when it is present, and an example. It was built from the code that fills the context, not from the docs: `RuntimeTaskInstance.get_template_context()` in `task-sdk/src/airflow/sdk/execution_time/task_runner.py`, the `Context` type in `task-sdk/src/airflow/sdk/definitions/context.py`, `task-sdk/src/airflow/sdk/execution_time/macros.py`, and the filters in `task-sdk/src/airflow/sdk/definitions/_internal/templater.py`. What it adds over the official templates reference: which keys are **conditional** and fail your template when absent, which documented keys are never actually set, and a replacement for every variable removed since 2.x.

---

## 1. How rendering works

| Fact | Detail |
| :--- | :--- |
| What gets rendered | Only an operator's `template_fields`. Examples: `BashOperator` renders `bash_command`, `env` and `cwd`; `PythonOperator` and `@task` render `templates_dict`, `op_args` and `op_kwargs`. Check an operator's `template_fields` before assuming a field is templated. |
| When and where | On the worker, just before `execute()`, with the context described below. Nothing is rendered at parse time. |
| Undefined names | The Dag's `template_undefined` defaults to `jinja2.StrictUndefined`. A missing variable is an **error** that fails the task, not an empty string. |
| Environment | A sandboxed Jinja environment with the `jinja2.ext.do` extension. Attributes starting with `_` are allowed, `__` are blocked. |
| Native types | `render_template_as_native_obj=True` on the Dag renders `"{{ params.ids }}"` to a real `list` instead of the string `"[1, 2]"`. |
| Template files | A field value ending in one of the operator's `template_ext` (for example `.sh` and `.bash` for `BashOperator`) is loaded as a file. The search path is the Dag file's folder plus `template_searchpath`. |
| Preview | `airflow tasks render <dag_id> <task_id> <logical_date_or_run_id>` prints every rendered field of an existing run without running it. |

---

## 2. The logical date trap

In Airflow 3, a Dag run can have **no logical date**. That happens for:

* asset-triggered runs (`schedule=[some_asset]`),
* manual runs created without one, which is the default for `airflow dags trigger` without `-l/--logical-date` and for API triggers that send `"logical_date": null`.

`get_template_context()` adds `logical_date`, `ds`, `ds_nodash`, `ts`, `ts_nodash`, `ts_nodash_with_tz`, `data_interval_start`, `data_interval_end`, `prev_data_interval_start_success` and `prev_data_interval_end_success` **only if** the run has a logical date. Combined with `StrictUndefined`, `{{ ds }}` in such a run fails the task with an `UndefinedError`. In TaskFlow, a parameter named `ds` receives `None` instead.

Ways to stay safe:

```python
# 1. Use what every run has.
"{{ run_id }}"
"{{ dag_run.run_after | ds }}"

# 2. Fall back explicitly.
"{{ (logical_date if logical_date is defined else dag_run.run_after) | ds }}"
```

> [!NOTE]
> `airflow dags test` and `dag.test()` default the logical date to "now", so a template that fails in production can pass every local test. Test with `dag.test(logical_date=None)` to reproduce a dateless run.

Also note that `ds` is not unique per Dag (asset and manual runs can share or lack one). Use `run_id` as an idempotency key.

---

## 3. Variables present in every task run

| Variable | Type | Example | Notes |
| :--- | :--- | :--- | :--- |
| `run_id` | `str` | `scheduled__2026-11-23T00:00:00+00:00` | Unique per Dag. Prefer it over `ds` for keys and paths. |
| `dag_run` | Dag run object | `{{ dag_run.run_after }}` | Fields: `dag_id`, `run_id`, `logical_date`, `data_interval_start`, `data_interval_end`, `run_after`, `start_date`, `end_date`, `clear_number`, `run_type`, `state`, `conf`, `triggering_user_name`, `consumed_asset_events`, `partition_key`, `partition_date`, `note`. |
| `dag` | `DAG` | `{{ dag.dag_id }}` | The Dag object as parsed on the worker. |
| `task` | operator | `{{ task.task_id }}`, `{{ task.owner }}` | The running operator. |
| `ti` / `task_instance` | runtime task instance | `{{ ti.try_number }}` | Fields: `id`, `dag_id`, `task_id`, `run_id`, `try_number`, `map_index`, `max_tries`, `hostname`, `start_date`, `log_url`; methods `xcom_pull(...)`, `xcom_push(key, value)`. |
| `params` | `dict[str, Any]` | `{{ params.region }}` | Dag and task `params`, merged with the run's conf and validated. |
| `var.value` / `var.json` | accessor | `{{ var.value.bucket }}` | Airflow Variables, see §8. |
| `conn` | accessor | `{{ conn.my_db.host }}` | Airflow Connections, see §8. |
| `macros` | accessor | `{{ macros.ds_add(ds, 7) }}` | See §7. |
| `task_instance_key_str` | `str` | `orders__extract__20261123` | `{dag_id}__{task_id}__{ds_nodash}`; for a run without logical date, the last part is the `run_id`. |
| `task_reschedule_count` | `int` | `0` | Times this try was rescheduled, for `mode="reschedule"` sensors. |
| `prev_start_date_success`, `prev_end_date_success` | `DateTime` or `None` | `{{ prev_start_date_success }}` | Start/end of the previous successful run, fetched lazily through the Execution API on first use. |
| `inlets`, `outlets` | `list` | `{{ outlets }}` | As declared on the task. |
| `inlet_events` | accessor | `inlet_events[my_asset][-1].extra` | Past events of inlet assets. |
| `outlet_events` | accessor | `outlet_events[my_asset].extra = {...}` | Attach `extra` to the events this task emits. Use in Python, not in a template. |
| `triggering_asset_events` | mapping, asset → list of events | `triggering_asset_events[my_asset][0].source_run_id` | Empty unless the run was triggered by assets. Each event has `asset`, `extra`, `source_dag_id`, `source_task_id`, `source_run_id`, `source_map_index`, `timestamp`, `partition_key`. |
| `partition_key`, `partition_date` | `str` / `DateTime`, or `None` | `{{ partition_key }}` | Set only for partitioned Dags; otherwise `None`. |
| `map_index_template` | `str` or `None` | | The task's `map_index_template`, used to label mapped instances. |
| `task_state_store` | accessor | | Per-task-instance key/value store, new in 3.4. Use from Python. |

---

## 4. Variables present only when the run has a logical date

| Variable | Type | Example value | Same as |
| :--- | :--- | :--- | :--- |
| `logical_date` | `pendulum.DateTime` | `2026-11-23T00:00:00+00:00` | |
| `ds` | `str` | `2026-11-23` | `{{ logical_date \| ds }}` |
| `ds_nodash` | `str` | `20261123` | `{{ logical_date \| ds_nodash }}` |
| `ts` | `str` | `2026-11-23T00:00:00+00:00` | `{{ logical_date \| ts }}` |
| `ts_nodash` | `str` | `20261123T000000` | `{{ logical_date \| ts_nodash }}` |
| `ts_nodash_with_tz` | `str` | `20261123T000000+0000` | `{{ logical_date \| ts_nodash_with_tz }}` |
| `data_interval_start` | `DateTime` or `None` | `2026-11-23T00:00:00+00:00` | `dag_run.data_interval_start` |
| `data_interval_end` | `DateTime` or `None` | `2026-11-23T00:00:00+00:00` | `dag_run.data_interval_end` |
| `prev_data_interval_start_success`, `prev_data_interval_end_success` | `DateTime` or `None` | | Data interval of the previous successful run, fetched lazily. |

> [!WARNING]
> For a cron or preset schedule such as `"@daily"`, 3.x uses `CronTriggerTimetable` by default (`[scheduler] create_cron_data_intervals` = `False`). The run that fires at midnight on 23 November has `logical_date` 23 November, and `data_interval_start` equals `data_interval_end`. In 2.x the same run had `ds` = 22 November and a one-day interval. Templates such as `WHERE day = '{{ ds }}'` silently select a different day after the upgrade. Set `create_cron_data_intervals = True`, or pass `CronDataIntervalTimetable` explicitly, to keep the 2.x meaning. See [scheduling, timetables & assets](../02_dag_authoring/02_scheduling_timetables_and_assets.md).

---

## 5. Context keys only in specific places

| Key | Where it exists |
| :--- | :--- |
| `exception` | Task failure callbacks (`on_failure_callback`, `on_retry_callback`): the error that ended the try. Also in the failure-email template context, together with `exception_html`, `try_number` and `max_tries`. |
| `reason` | Dag-level `on_success_callback` / `on_failure_callback`, run by the Dag processor: the message explaining the run's final state. |
| `templates_dict` | Only for `PythonOperator` and `@task`, when you pass `templates_dict=`; its values are rendered. |
| `asset_state_store` | Only when the task has an `Asset`, asset reference or `AssetAlias` among its inlets or outlets. |
| `loop` | Only inside a loop task group; a TaskFlow function must take it as a keyword-only parameter. |

**Declared but not set.** The `Context` type also lists `test_mode`, `try_number` and `expanded_ti_count`, but the 3.4 task context never sets them (the `test_mode` line is commented out in `get_template_context()`). In a template they are undefined and fail. Use `ti.try_number` and `ti.expanded_ti_count` instead; there is no replacement for `test_mode`.

---

## 6. Filters

Defined in `templater.py` and available in every template. Each returns `None` for a `None` input.

| Filter | Output for `2026-11-23T06:30:00+00:00` |
| :--- | :--- |
| `ds` | `2026-11-23` |
| `ds_nodash` | `20261123` |
| `ts` | `2026-11-23T06:30:00+00:00` |
| `ts_nodash` | `20261123T063000` |
| `ts_nodash_with_tz` | `20261123T063000+0000` |

The filters work on any datetime, so `{{ data_interval_end | ds }}` and `{{ dag_run.run_after | ts_nodash }}` are valid. Add your own with `user_defined_filters` (§10).

---

## 7. Macros

`{{ macros.<name> }}` resolves attributes of `airflow.sdk.execution_time.macros` lazily.

| Macro | Signature | Example | Result |
| :--- | :--- | :--- | :--- |
| `ds_add` | `ds_add(ds: str, days: int) -> str` | `{{ macros.ds_add('2026-11-23', -1) }}` | `2026-11-22` |
| `ds_format` | `ds_format(ds: str, input_format: str, output_format: str) -> str` | `{{ macros.ds_format(ds, '%Y-%m-%d', '%d/%m/%Y') }}` | `23/11/2026` |
| `ds_format_locale` | `ds_format_locale(ds, input_format, output_format, locale=None) -> str` | `{{ macros.ds_format_locale(ds, '%Y-%m-%d', 'EEEE d MMMM', 'de_DE') }}` | `Montag 23 November` |
| `datetime_diff_for_humans` | `datetime_diff_for_humans(dt, since=None) -> str` | `{{ macros.datetime_diff_for_humans(dag_run.start_date) }}` | `5 minutes ago` |
| `datetime` | `datetime.datetime` class | `{{ macros.datetime(2026, 1, 1) }}` | |
| `timedelta` | `datetime.timedelta` class | `{{ (logical_date + macros.timedelta(days=1)) \| ds }}` | next day |
| `dateutil` | the `dateutil` package | | |
| `time` | the `time` module | `{{ macros.time.time() }}` | epoch seconds |
| `uuid` | the `uuid` module | `{{ macros.uuid.uuid4() }}` | |
| `random` | the `random.random` **function** | `{{ macros.random() }}` | a float in [0, 1) |
| `json` | the `json` module | `{{ macros.json.dumps(dag_run.conf) }}` | |
| `yaml` | `airflow.sdk.yaml` (safe loader and dumper) | `{{ macros.yaml.dump(dag_run.conf) }}` | |

Notes:
* `ds_format_locale` uses Babel patterns (`yyyy-MM-dd`, `EEEE`) for the output, not `strftime` codes. The docstring of `ds_format` in 3.4 shows a four-argument example that the function does not accept; it takes exactly three.
* `macros.random` is the function itself, not the module, so `macros.random.randint` doesn't exist.
* Plugin macros appear as `{{ macros.<plugin_name>.<macro>() }}`. With multi-team enabled, a plugin owned by another team raises `AttributeError`.

---

## 8. Variables and Connections in templates

| Expression | Returns |
| :--- | :--- |
| `{{ var.value.my_key }}` | The Variable as a string. |
| `{{ var.json.my_key.nested.field }}` | The Variable parsed as JSON. |
| `{{ var.value.get('my.dotted.key', 'fallback') }}` | Lookup by string, with a default when the Variable doesn't exist. |
| `{{ conn.my_conn_id.host }}` | A Connection attribute (`host`, `login`, `password`, `port`, `schema`, `extra_dejson`). |
| `{{ conn.get('my_conn_id').port }}` | Lookup by string; returns `None` (or the given default) when missing. |

Each lookup is a call from the worker to the API server (or a configured secrets backend), made at render time. They are fine in a template; the same calls at the **top level** of a Dag file run on every parse, which is an [anti-pattern](../01_architecture_and_mental_model/02_anti_patterns.md). Values of Connections and sensitive Variables are masked in logs, but a rendered template that embeds a password still shows it in the UI's **Rendered Template** tab. Prefer passing a `conn_id` to the operator. See [XCom, Variables & Connections](../03_task_execution/04_xcom_variables_and_connections.md).

---

## 9. Removed since Airflow 2.x

None of these exist in the 3.4 context. A template that uses one fails with `UndefinedError`; a TaskFlow function that declares one as a parameter doesn't get it injected.

| 2.x variable | 3.4 replacement |
| :--- | :--- |
| `execution_date` | `logical_date` (absent on dateless runs, see §2) |
| `next_execution_date` | `data_interval_end` when using an interval timetable; otherwise compute it: `logical_date + macros.timedelta(days=1)` |
| `next_ds`, `next_ds_nodash` | `{{ data_interval_end \| ds }}` / `{{ data_interval_end \| ds_nodash }}` with an interval timetable |
| `prev_execution_date` | `data_interval_start` with an interval timetable, or `prev_data_interval_start_success` |
| `prev_ds`, `prev_ds_nodash` | `{{ data_interval_start \| ds }}`, or `{{ macros.ds_add(ds, -1) }}` for a daily Dag |
| `prev_execution_date_success` | `prev_data_interval_start_success` or `prev_start_date_success` |
| `tomorrow_ds`, `tomorrow_ds_nodash` | `{{ macros.ds_add(ds, 1) }}`; for no dashes, `{{ (logical_date + macros.timedelta(days=1)) \| ds_nodash }}` |
| `yesterday_ds`, `yesterday_ds_nodash` | `{{ macros.ds_add(ds, -1) }}`; for no dashes, `{{ (logical_date - macros.timedelta(days=1)) \| ds_nodash }}` |
| `conf` (the Airflow configuration) | Not in the context any more. Read configuration in Python with `from airflow.sdk import conf`. |

The upgrade guide in `airflow-core/docs/installation/upgrading_to_airflow3.rst` lists the first group; `conf` is absent from the 3.4 `Context` type. Check old Dags with `airflow tasks render` against a real run before you cut over. See [upgrading to Airflow 3](../05_deployment_and_config/04_upgrading_to_airflow_3.md).

---

## 10. Custom macros and filters

```python
import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


def fiscal_quarter(d) -> str:
    return f"FY{d.year}Q{(d.month - 1) // 3 + 1}"


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    user_defined_macros={"warehouse": "analytics", "fiscal_quarter": fiscal_quarter},
    user_defined_filters={"upper_snake": lambda s: s.upper().replace("-", "_")},
)
def templated_report():
    BashOperator(
        task_id="report",
        bash_command=(
            "echo run={{ run_id | upper_snake }} "
            "db={{ warehouse }} "
            "quarter={{ fiscal_quarter(dag_run.run_after) }} "
            "day={{ (logical_date if logical_date is defined else dag_run.run_after) | ds }}"
        ),
    )


templated_report()
```

* `user_defined_macros` become **top-level** names (`{{ warehouse }}`), not `{{ macros.warehouse }}`. A name that collides with a context key is shadowed by the context.
* `user_defined_filters` add to the five built-in filters.
* `BashOperator` lives in the `apache-airflow-providers-standard` package; provider versions are released independently of core.
* For macros shared by many Dags, use a plugin (`macros` attribute of an `AirflowPlugin`), reached as `{{ macros.<plugin_name>.<macro>() }}`.

---

## 11. The same values from Python

In a TaskFlow function, declare a parameter with the context key's name, and Airflow fills it (it must have no default, or a default of `None`):

```python
from airflow.sdk import get_current_context, task


@task
def show(run_id, params, ti, logical_date=None):
    print(run_id, params, ti.try_number, logical_date)


@task
def show_all():
    context = get_current_context()
    print(sorted(context.keys()))
```

* A key missing from the context (`logical_date` on a dateless run) arrives as `None`, not as an error. Check for it.
* `get_current_context()` only works while a task is executing; called at parse time it fails.
* String arguments to a `@task` call are rendered too: `op_args` and `op_kwargs` are template fields of the decorated operator in the standard provider, so a call such as `load(path="data/{{ run_id }}.json")` receives the rendered path. The official templates reference says the opposite; the source (`providers/standard/src/airflow/providers/standard/decorators/python.py`) supports rendering. Prefer the context parameter, which is explicit.

See the [Task SDK quick reference](06_task_sdk_quick_reference.md) for `get_current_context`, `Param` and the rest of `airflow.sdk`, and the [Dag authoring tutorial](../00_getting_started/02_tutorial_dag_authoring.md) for these values in a working pipeline.
