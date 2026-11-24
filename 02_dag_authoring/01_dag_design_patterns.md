# Dag Design Patterns

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Dags](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/dags.html) · [TaskFlow](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/taskflow.html) · [Mapped tasks](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/dynamic-task-mapping.html) · [Setup and Teardown](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/setup-and-teardown.html)

The official docs explain each authoring feature separately. This page is the opinionated version: which style to pick, how to make tasks safe to rerun, how to set retries and timeouts, and which building blocks (trigger rules, branching, task groups, setup/teardown, mapping, params, callbacks) solve which problem, ending with skeletons for an ETL and an ML pipeline. All examples import from `airflow.sdk` and were checked against the 3.4 Task SDK (`task-sdk/src/airflow/sdk/`). Operators such as `BashOperator` come from the standard provider, which core 3.4 installs as a dependency; provider versions are independent of core.

---

## 1. TaskFlow or classic operators

| Use | When |
| :--- | :--- |
| TaskFlow (`@dag`, `@task`) | Python logic you write yourself; passing small values between tasks; mapping over results |
| Classic operators (`BashOperator`, `SQLExecuteQueryOperator`, provider operators) | Calling an external system that a provider already wraps, with retries, templating, deferrable mode and links |
| Both in one Dag | Common. An `XComArg` (the return of a `@task` call) can be passed into an operator's templated field |

```python
import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(schedule="@daily", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False, tags=["example"])
def mixed_styles():
    @task
    def pick_table() -> str:
        return "orders"

    table = pick_table()
    # str(XComArg) renders as an xcom_pull template; the dependency must still be set explicitly
    table >> BashOperator(task_id="count_rows", bash_command=f"echo counting {table}")


mixed_styles()
```

Rules of thumb:

* Call the `@dag` function at module level (`mixed_styles()` above). Without that call no Dag is registered.
* A `@task` function's name is its default `task_id`. Renaming the function renames the task (see [anti-patterns](../01_architecture_and_mental_model/02_anti_patterns.md#4-renaming-a-dag_id-or-task_id)).
* Return values become XComs, stored through the API server. Return references (paths, ids), not data.
* Need different Python dependencies for one task? `@task.virtualenv`, `@task.external_python`, `@task.docker` or `@task.kubernetes` (the last two come from their providers).

---

## 2. Idempotent tasks

A task is idempotent when running it twice for the same Dag run leaves the same result as running it once. Airflow reruns tasks on retries, clears, backfills and lost workers, so this is not optional.

| Do | Instead of |
| :--- | :--- |
| Write to a location keyed by the run: `.../dt={{ data_interval_start \| ds }}/` or `.../{{ run_id }}/` | Appending to "today's" file |
| `MERGE` / upsert, or delete-then-insert in one transaction | Plain `INSERT` |
| Take dates from the context (`data_interval_start`, `dag_run.run_after`) | `datetime.now()` inside the task |
| Make external side effects conditional ("create if not exists") | Fire-and-forget API calls |

Which date to key on:

* Scheduled runs with a data-interval timetable: `data_interval_start` / `data_interval_end`.
* The 3.x default cron timetable gives a zero-length interval; use `logical_date` or pass `interval=` to `CronTriggerTimetable` (see [scheduling](02_scheduling_timetables_and_assets.md)).
* Manual and asset-triggered runs may have no `logical_date` at all. `dag_run.run_after` is always set.

```python
from airflow.sdk import task


@task
def load(**context) -> str:
    dag_run = context["dag_run"]
    anchor = context.get("data_interval_start") or dag_run.run_after
    target = f"s3://example-bucket/orders/dt={anchor:%Y-%m-%d}/"
    # delete target, then write it: rerunning gives the same result
    return target
```

---

## 3. Retries, timeouts and failure handling

### Retries

| Argument (on any task) | Default | Notes |
| :--- | :--- | :--- |
| `retries` | `[core] default_task_retries` = 0 | Set it per Dag in `default_args` |
| `retry_delay` | `[core] default_task_retry_delay` = 300 s | `timedelta` or seconds |
| `retry_exponential_backoff` | `0` (off) | A **multiplier** in 3.4 (float). `2.0` doubles the delay each try |
| `max_retry_delay` | `[core] max_task_retry_delay` = 86400 s | Cap for backoff |
| `retry_policy` | `None` | Per-exception decisions (below) |

With backoff, the delay for try *n* is about `retry_delay * multiplier^(n-1)`, with a deterministic per-task-instance jitter between that value and twice it (`TaskInstance._compute_retry_deadline` in `airflow-core/src/airflow/models/taskinstance.py`).

**Retry policies.** Retrying a bad password five times wastes time; retrying a rate limit immediately makes it worse. `retry_policy` (exported from `airflow.sdk`) decides per exception, in the worker:

```python
from datetime import timedelta

from airflow.sdk import ExceptionRetryPolicy, RetryAction, RetryRule, task


@task(
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_policy=ExceptionRetryPolicy(
        rules=[
            RetryRule(exception=PermissionError, action=RetryAction.FAIL, reason="credentials are wrong"),
            RetryRule(
                exception=["requests.exceptions.HTTPError", TimeoutError],
                action=RetryAction.RETRY,
                retry_delay=timedelta(minutes=10),
            ),
        ],
    ),
)
def call_api() -> None: ...
```

Rules are checked in order; the first match wins. `RetryAction.FAIL` fails immediately, `RETRY` retries (never beyond `retries`), `DEFAULT` falls through, and `RESCHEDULE` runs the task again without using a retry but needs `max_reschedules` or `reschedule_timeout`. `AirflowFailException` and `AirflowSensorTimeout` always fail without consulting the policy (`task-sdk/src/airflow/sdk/definitions/retry_policy.py`).

### Timeouts

| Setting | Scope | What happens |
| :--- | :--- | :--- |
| `execution_timeout` (task) | One try | The task raises `AirflowTaskTimeout` and fails (or retries). Default `[core] default_task_execution_timeout` is empty: **no limit** |
| `timeout` (sensor) | Whole wait across pokes | Sensor fails with `AirflowSensorTimeout`, no retry. Default `[sensors] default_timeout` = 7 days |
| `dagrun_timeout` (Dag) | Whole run | Scheduler marks the run `failed` and sets unfinished tasks to `skipped` |

Set `execution_timeout` on every task that talks to an external system. A hung task with no timeout holds a slot until the heartbeat machinery or a human notices.

### Failing fast

* `raise AirflowFailException("...")` (from `airflow.sdk.exceptions`) fails the task with no retries. `raise AirflowSkipException` marks it `skipped`.
* `DAG(fail_fast=True)` fails the other running tasks when one task fails. All tasks must keep the default trigger rule, or the Dag fails validation.

---

## 4. Trigger rules

`trigger_rule` decides when a task may run based on its direct upstreams. The full list in 3.4 (`TriggerRule` in `airflow.sdk`):

| Rule | Runs when |
| :--- | :--- |
| `all_success` (default) | All upstreams succeeded |
| `all_failed` | All upstreams are `failed` or `upstream_failed` |
| `all_done` | All upstreams finished, whatever the state |
| `all_done_min_one_success` | All upstreams finished and at least one succeeded |
| `all_done_setup_success` | All finished and at least one setup succeeded (used for teardowns) |
| `all_skipped` | All upstreams skipped |
| `one_success` | At least one upstream succeeded (does not wait for the rest) |
| `one_failed` | At least one upstream failed (does not wait) |
| `one_done` | At least one upstream succeeded or failed |
| `none_failed` | No upstream failed or upstream_failed (skips are fine) |
| `none_failed_min_one_success` | No failures and at least one success. **Use this after a branch** |
| `none_skipped` | No upstream skipped |
| `always` | Immediately, regardless of upstreams |

> [!TIP]
> For "notify on failure", prefer an `on_failure_callback` or a Dag-level callback over a task with `one_failed`: the task version marks the run as having a skipped or extra task and is easy to break when the graph changes.

---

## 5. Branching and short-circuiting

```python
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task


@task.branch
def choose(**context) -> str:
    return "full_load" if context["params"].get("full") else "incremental_load"


with DAG("branching_example", schedule=None, params={"full": False}):
    full = EmptyOperator(task_id="full_load")
    incremental = EmptyOperator(task_id="incremental_load")
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    choose() >> [full, incremental] >> join
```

The branch returns one task id or a list; the others are skipped, and the skip propagates downstream unless a task's trigger rule tolerates it, which is why `join` needs `none_failed_min_one_success`. `@task.short_circuit` returns a boolean: `False` skips everything downstream. Both come from the standard provider.

---

## 6. Task groups

Task groups are a UI and dependency-wiring tool. They have no runtime or scheduling effect of their own, and they replace Airflow 2's SubDags.

```python
from airflow.sdk import task, task_group


@task_group
def extract_source(source: str):
    @task
    def pull(src: str) -> str:
        return f"s3://example-bucket/raw/{src}"

    @task
    def validate(path: str) -> str:
        return path

    return validate(pull(source))
```

Task ids inside a group are prefixed with the group id (`extract_source.pull`) unless `prefix_group_id=False`. A `@task_group` can also be mapped with `.expand()`.

---

## 7. Setup and teardown

Use these for resources that must be cleaned up even when the work fails: a temporary cluster, a scratch schema, a lock.

```python
from airflow.sdk import DAG, setup, task, teardown


@setup
def create_cluster() -> str:
    return "cluster-1"


@task
def run_query(cluster_id: str) -> None: ...


@teardown
def delete_cluster(cluster_id: str) -> None: ...


with DAG("setup_teardown_example", schedule=None):
    cluster = create_cluster()
    run_query(cluster) >> delete_cluster(cluster).as_teardown(setups=cluster)
```

Behavior (official setup/teardown how-to, and `as_teardown` in `task-sdk/src/airflow/sdk/definitions/xcom_arg.py`):

* The teardown runs if its setup succeeded, even when the work failed.
* By default teardowns do not decide the Dag run state; pass `on_failure_fail_dagrun=True` to `@teardown` to change that.
* Clearing a work task also clears its setup and teardown, so the resource is recreated.

---

## 8. Dynamic task mapping

Create one task instance per element at run time:

```python
from airflow.sdk import DAG, get_current_context, task


@task
def list_files() -> list[str]:
    return ["a.csv", "b.csv"]


@task(map_index_template="{{ file_name }}", max_active_tis_per_dagrun=8)
def process(file: str, bucket: str) -> int:
    get_current_context()["file_name"] = file  # read by map_index_template after the task runs
    return len(file)


with DAG("mapping_example", schedule=None):
    process.partial(bucket="example-bucket").expand(file=list_files())
```

| Method | Use |
| :--- | :--- |
| `.partial(**kwargs)` | Arguments that are the same for every mapped instance (also `task_id`, `pool`, `retries`) |
| `.expand(arg=iterable)` | Map over one or more arguments (several arguments give a cross product) |
| `.expand_kwargs(list_of_dicts)` | Each element is a full set of keyword arguments: zip-like mapping |
| `map_index_template` | Jinja template that names each instance in the UI instead of 0, 1, 2. It is rendered after the task runs against the task context, so put the value there (as above), or use `{{ task.<attribute> }}` for classic operators |

Limits: an upstream return value longer than `[core] max_map_length` (1024) is rejected by the API server and the upstream task fails. Use `max_active_tis_per_dagrun` to cap how many mapped instances run at once. Classic operators use the same API: `BashOperator.partial(task_id="echo").expand(bash_command=[...])`.

---

## 9. Params and run configuration

```python
import pendulum

from airflow.sdk import Param, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    params={
        "full": Param(False, type="boolean"),
        "region": Param("eu", enum=["eu", "us"]),
        "limit": Param(1000, type="integer", minimum=1),
    },
)
def parametrized():
    @task
    def show(**context) -> None:
        print(context["params"]["region"], context["dag_run"].conf)

    show()


parametrized()
```

* `params` are validated with JSON Schema when a run is created; the UI's trigger form is generated from them.
* Values passed as run conf (`airflow dags trigger -c '{"region": "us"}' parametrized`, the REST API, or the UI form) override params because `[core] dag_run_conf_overrides_params` is `True` by default.
* Read them as `context["params"]` or `{{ params.region }}`; the raw conf is `dag_run.conf`.
* Scheduled runs use the defaults, so every param needs a valid default.

---

## 10. Callbacks, notifiers and deadline alerts

| Hook | Where it runs | Use it for |
| :--- | :--- | :--- |
| Task `on_failure_callback`, `on_retry_callback`, `on_success_callback`, `on_skipped_callback`, `on_execute_callback` | Worker (Dag processor if the worker died) | Alerting on a specific task |
| Dag `on_failure_callback` / `on_success_callback` | Dag processor | One alert per run |
| `BaseNotifier` subclasses (Slack, SMTP and other provider notifiers) | Same as the callback slot you put them in | Reusable, templated alerts |
| `DAG(deadline=DeadlineAlert(...))` | Triggerer (`AsyncCallback`) or an executor (`SyncCallback`) | "This run is late", replacing Airflow 2 SLAs |

```python
from datetime import timedelta

from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback

deadline = DeadlineAlert(
    reference=DeadlineReference.DAGRUN_QUEUED_AT,
    interval=timedelta(hours=2),
    callback=SyncCallback("my_company.alerts.notify_late_run"),
)
```

The callback path must be importable where it runs. Keep callbacks short and failure-proof: an exception in a task callback is logged ("Failed to run task callback") and not retried (`_run_task_state_change_callbacks` in `task_runner.py`).

---

## 11. Dag-level defaults

Put cross-cutting settings in one place:

```python
from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": 2.0,
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=1),
}
```

Pass it as `default_args=DEFAULT_ARGS`. A value set on a task wins over `default_args`. Dag-level limits go on the Dag itself: `max_active_runs`, `max_active_tasks`, `dagrun_timeout`, `max_consecutive_failed_dag_runs`, `catchup`, `tags`.

---

## 12. Pattern: daily ETL

```python
from datetime import timedelta

import pendulum

from airflow.sdk import Asset, CronDataIntervalTimetable, dag, task

ORDERS_CLEAN = Asset("orders_clean", uri="s3://example-bucket/clean/orders/")


@dag(
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "execution_timeout": timedelta(hours=1)},
)
def orders_etl():
    @task
    def extract(data_interval_start=None, data_interval_end=None) -> str:
        return f"s3://example-bucket/raw/orders/{data_interval_start:%Y-%m-%d}/"

    @task
    def transform(raw_path: str) -> str:
        return raw_path.replace("/raw/", "/staged/")

    @task(outlets=[ORDERS_CLEAN])
    def load(staged_path: str) -> None:
        """Overwrite the target partition, so reruns are safe."""

    load(transform(extract()))


orders_etl()
```

Why these choices: a data-interval timetable because the job processes "yesterday"; `max_active_runs=1` so backfilled days do not overlap; the outlet publishes an asset event that downstream Dags can schedule on.

---

## 13. Pattern: ML training pipeline

* Trigger on data, not time: `schedule=ORDERS_CLEAN` (or `ORDERS_CLEAN | another_asset`).
* Run heavy steps out of the scheduler's Python: `@task.kubernetes`, `@task.docker`, or a provider operator for a managed training service. Keep framework imports inside tasks.
* Map over hyperparameter sets with `.expand_kwargs()`, cap with `max_active_tis_per_dagrun`, then pick the best with a single downstream task.
* Put GPU-bound tasks in a small pool so they cannot starve other work ([pools](../03_task_execution/03_concurrency_pools_and_priority.md)).
* Store models and metrics in object storage and pass the URI. Publish an `Asset` for the registered model so serving Dags can react.
* Use a teardown for any cluster you create.

Next: [scheduling, timetables and assets](02_scheduling_timetables_and_assets.md), then [testing Dags](03_testing_dags.md).
