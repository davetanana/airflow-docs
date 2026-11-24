# Scheduling, Timetables and Assets

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Timetables](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/timetable.html) · [Asset Definitions](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/assets.html) · [Asset-Aware Scheduling](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/asset-scheduling.html) · [Event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/3.4.0/authoring-and-scheduling/event-scheduling.html) · [Backfill](https://airflow.apache.org/docs/apache-airflow/3.4.0/core-concepts/backfill.html)

Scheduling is where Airflow 2 knowledge does the most damage in Airflow 3: a cron string means something different, catchup is off, asset events no longer batch by default, and backfills are run by the scheduler. This page gives the 3.4 rules in one place, with the config keys that change them, then covers assets, event-driven scheduling with watchers, and partitioned Dags. Everything was checked against `task-sdk/src/airflow/sdk/definitions/` and `config.yml` on `v3-4-stable`. For what the dates on a run mean, read [the mental model, §6](../01_architecture_and_mental_model/01_mental_model.md#6-dates-on-a-dag-run-logical_date-data-interval-run_after) first.

---

## 1. The `schedule` argument

`schedule` is the only scheduling argument in 3.x (`schedule_interval` and `timetable` are gone). How each value is turned into a timetable (`_create_timetable` and `DAG._default_timetable` in `task-sdk/src/airflow/sdk/definitions/dag.py`):

| You pass | You get | Notes |
| :--- | :--- | :--- |
| nothing / `None` | `NullTimetable` | **The default.** Manual, API and backfill runs only |
| `"@once"` | `OnceTimetable` | One run, then nothing |
| `"@continuous"` | `ContinuousTimetable` | A new run as soon as the previous one ends. The timetable's `active_runs_limit` makes `max_active_runs` default to 1, and a higher value fails validation |
| A cron string or preset (`"0 2 * * *"`, `"@daily"`, `"@hourly"`) | `CronTriggerTimetable` by default; `CronDataIntervalTimetable` if `[scheduler] create_cron_data_intervals = True` | See §2 |
| `timedelta(...)` / `relativedelta(...)` | `DeltaTriggerTimetable` by default; `DeltaDataIntervalTimetable` if `[scheduler] create_cron_data_intervals = True` | Yes, the **cron** key (see warning below) |
| A timetable instance | That timetable | `CronTriggerTimetable`, `MultipleCronTriggerTimetable`, `CronDataIntervalTimetable`, `DeltaTriggerTimetable`, `DeltaDataIntervalTimetable`, `EventsTimetable`, `CronPartitionTimetable`, asset timetables, or a plugin's custom timetable |
| An `Asset`, `AssetAlias` or condition (`a \| b`, `a & b`) | `AssetTriggeredTimetable(condition)` | See §5 |
| A list of assets | `AssetTriggeredTimetable(AssetAll(...))` | A list means **all of them** |

> [!WARNING]
> `[scheduler] create_delta_data_intervals` exists in `config.yml` (default `False`) but the 3.4 Task SDK never reads it: timedelta schedules follow `[scheduler] create_cron_data_intervals` (`_create_timetable` in `dag.py`). The 3.4 timetable docs say the same in a note. To get data intervals for one Dag only, pass `DeltaDataIntervalTimetable(...)` explicitly instead of flipping either key.

Check what the scheduler will do next:

```bash
airflow dags next-execution <dag_id>     # replace <dag_id>; prints upcoming logical dates
airflow dags details <dag_id>            # shows timetable, catchup, next run fields
```

---

## 2. Trigger timetables vs data-interval timetables

| | `CronTriggerTimetable` (3.x default) | `CronDataIntervalTimetable` (Airflow 2 behavior) |
| :--- | :--- | :--- |
| Run for the 02:00 tick happens | at 02:00 | at the **next** tick, when the interval ends |
| `logical_date` | the tick (02:00 today) | start of the interval (02:00 yesterday) |
| Data interval | zero length (start = end = tick) unless you pass `interval=` | the whole period between ticks |
| Good for | "run at this time", like cron | "process the previous period" ETL |

```python
from datetime import timedelta

from airflow.sdk import CronDataIntervalTimetable, CronTriggerTimetable

# Runs at 02:00 and processes the 24 hours before it:
trigger_with_window = CronTriggerTimetable("0 2 * * *", timezone="UTC", interval=timedelta(days=1))

# Airflow 2 semantics: the run for interval [Mon 02:00, Tue 02:00) starts on Tue 02:00:
interval_style = CronDataIntervalTimetable("0 2 * * *", timezone="UTC")
```

`CronTriggerTimetable` also takes `run_immediately` (`False` by default): whether the first run of a Dag with no `start_date` happens at the most recent tick or waits for the next one. `MultipleCronTriggerTimetable("0 6 * * 1-5", "0 10 * * 6,0", timezone="UTC")` combines several crons into one schedule.

> [!IMPORTANT]
> Flipping `[scheduler] create_cron_data_intervals` changes the meaning of every Dag that uses a plain cron string, and takes effect when Dags are re-parsed. When a Dag switches from the trigger to the data-interval timetable, the next run skips one period to avoid colliding with the last trigger-style logical date (per the key's description). Prefer explicit timetable classes in Dags whose dates matter.

---

## 3. `start_date`, `end_date` and catchup

* `start_date` is optional in 3.x, except that `catchup=True` without one fails with `start_date is required when catchup=True`. Use a fixed, timezone-aware value (`pendulum.datetime(2026, 1, 1, tz="UTC")`); a naive `datetime` is taken to be in `[core] default_timezone` *(not verified against source)*. Never compute it from the current time ([anti-patterns §3](../01_architecture_and_mental_model/02_anti_patterns.md#3-non-deterministic-dag-definitions)).
* `end_date` stops scheduling after that date.
* `catchup` defaults to `[scheduler] catchup_by_default`, which is `False`. With `catchup=False`, a newly unpaused Dag gets one run for the most recent tick, then follows the schedule. With `catchup=True`, it gets one run per missed tick since `start_date` (or since the last run), throttled by `max_active_runs`.
* New Dags start paused (`[core] dags_are_paused_at_creation = True`); override per Dag with `is_paused_upon_creation`.

> [!TIP]
> Leave catchup off and fill history with an explicit backfill (§4). A backfill has a dry run, its own concurrency limit and a visible record; catchup has none of those.

---

## 4. Backfills in 3.x

In Airflow 3 a backfill is a database object (`backfill` and `backfill_dag_run` tables). The CLI or REST API creates it; **the scheduler** creates and runs the Dag runs. The CLI does not run tasks and returns immediately.

```bash
# Preview: which logical dates (or partitions) would be attempted
airflow backfill create --dag-id orders_etl --from-date 2026-01-01 --to-date 2026-01-31 --dry-run

# Create it: rerun failed days too, at most 3 runs at a time
airflow backfill create --dag-id orders_etl --from-date 2026-01-01 --to-date 2026-01-31 --reprocess-behavior failed --max-active-runs 3
```

| Flag | Meaning |
| :--- | :--- |
| `--from-date`, `--to-date` | Required, both inclusive. For partitioned Dags they are read as a partition-date range |
| `--reprocess-behavior` | `none` (default: only missing runs), `failed` (missing and failed), `completed` (all). A run that is still queued or running is never duplicated |
| `--max-active-runs` | Concurrency of this backfill, independent of the Dag's `max_active_runs` |
| `--run-backwards` | Newest first. Not supported when tasks use `depends_on_past` |
| `--dag-run-conf` | JSON conf for every created run |
| `--run-on-latest-version` / `--no-run-on-latest-version` | Code version for the runs. Unset falls back to `[core] rerun_with_latest_version`, then to True for backfills |
| `--dry-run` | Print the candidate runs only |

The command fails if the Dag has no time-based schedule, if `allowed_run_types` excludes backfills, if there is already an active backfill for the Dag, or if no runs would be created (exit code 1 with a warning). Backfill runs of a paused Dag wait until it is unpaused; the UI's backfill form also offers to unpause or drain the Dag. Pause, unpause or cancel a running backfill from the UI or REST API.

---

## 5. Assets

An asset is a named piece of data that tasks produce and Dags wait for. The scheduler never looks at the data; it only records **asset events** that tasks emit when they succeed.

```python
import pendulum

from airflow.sdk import Asset, AssetAlias, Metadata, dag, task

raw_orders = Asset("raw_orders", uri="s3://example-bucket/raw/orders/")
fx_rates = Asset("fx_rates", uri="s3://example-bucket/fx/")


@dag(schedule="@hourly", start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), catchup=False)
def producer():
    @task(outlets=[raw_orders])
    def land_orders():
        yield Metadata(raw_orders, {"row_count": 1250})  # becomes the event's extra

    land_orders()


@dag(schedule=(raw_orders & fx_rates), catchup=False)
def consumer():
    @task(inlets=[raw_orders])
    def read(**context):
        last = context["inlet_events"][raw_orders][-1]
        print(last.extra.get("row_count"), context["triggering_asset_events"])

    read()


producer()
consumer()
```

| Concept | How |
| :--- | :--- |
| Identity | `Asset(name, uri=..., group=..., extra=...)`. Name or URI alone also works. The name is the key the UI shows |
| Produce | List the asset in a task's `outlets`. An event is recorded when the task **succeeds** |
| Add data to the event | `yield Metadata(asset, {...})`, or `context["outlet_events"][asset].extra = {...}`. JSON-serializable values only |
| Consume | `schedule=asset`, `schedule=(a \| b)`, `schedule=(a & (b \| c))`; `AssetAny` / `AssetAll` are the function forms |
| Read past events | Declare `inlets=[asset]`, then `context["inlet_events"][asset]`: a lazy, time-ordered sequence |
| See what triggered the run | `context["triggering_asset_events"]` / `{{ triggering_asset_events }}` |
| Decide the target at run time | Put an `AssetAlias("name")` in `outlets`, then `context["outlet_events"][alias].add(Asset(...), extra={...})`. Consumers can schedule on the alias |
| Asset-first style | `@asset(schedule=..., uri=...)` defines a one-task Dag that produces the asset named after the function |

CLI helpers: `airflow assets list`, `airflow assets details --name raw_orders`, and `airflow assets materialize --name raw_orders` (runs the Dag that produces it).

---

## 6. One run per event, or batched (new in 3.4)

> [!IMPORTANT]
> Airflow 2 and 3.0 to 3.3 merged all queued events for an asset-triggered Dag into one run. In 3.4, `[scheduler] batch_asset_events` defaults to `False`: **each asset event creates its own Dag run**. Existing serialized Dags keep batching until they are re-parsed.

* Pass `batch_asset_events=True` (or `False`) to `AssetTriggeredTimetable`, `AssetOrTimeSchedule` or `PartitionedAssetTimetable` to decide per Dag; the explicit argument wins over the config key.
* AND conditions (`a & b`) and rollup partition mappers always batch. Passing `batch_asset_events=False` for them fails validation with `Asset AND conditions require batch_asset_events=True` (`definitions/timetables/assets.py`).
* Per-event runs respect `max_active_runs` and `[scheduler] max_dagruns_to_create_per_loop`; leftover events stay queued.
* `catchup=False` does not discard a queued backlog for asset-triggered Dags.

```python
from airflow.sdk import Asset, AssetTriggeredTimetable, dag


@dag(schedule=AssetTriggeredTimetable(assets=Asset("raw_orders"), batch_asset_events=True), catchup=False)
def refresh_dashboard(): ...


refresh_dashboard()
```

Use batching for "rebuild from the latest state" consumers; keep per-event runs when each event is a separate unit of work (a file, a partition).

### Mixing time and assets

| Timetable | Behavior |
| :--- | :--- |
| `AssetOrTimeSchedule(timetable=..., assets=...)` | Runs on the time schedule **and** whenever the assets update, independently |
| `AssetAndTimeSchedule(timetable=..., assets=...)` | Runs on the time schedule, but each scheduled run waits until the assets have been updated |

---

## 7. Partitioned Dags

Since 3.2, an asset event can carry a `partition_key`, and a Dag run can have `partition_key` / `partition_date` instead of (or as well as) a logical date. Partitioned events only trigger partition-aware consumers.

| Role | Timetable | Key decided |
| :--- | :--- | :--- |
| Producer, key follows the schedule | `CronPartitionTimetable("15 * * * *", timezone="UTC")` | Before the run, from the cron tick |
| Producer, key known only at run time | `PartitionedAtRuntime()` (never schedules on its own); the task calls `context["outlet_events"][asset].add_partitions([...])` | During the run |
| Consumer | `PartitionedAssetTimetable(assets=..., default_partition_mapper=..., partition_mapper_config={...})` | Upstream keys mapped to downstream keys |

Partition mappers (all exported from `airflow.sdk`): `IdentityMapper` (default), `StartOfHourMapper` / `StartOfDayMapper` / `StartOfWeekMapper` / `StartOfMonthMapper` / `StartOfQuarterMapper` / `StartOfYearMapper` (time grain), `ProductMapper` (composite keys like `us|2026-03-10T09:00:00`), `AllowedKeyMapper`, `FixedKeyMapper`, `ChainMapper`, `FanOutMapper`, and `RollupMapper` with windows (`DayWindow`, `SegmentWindow`, ...) and wait policies (`WaitForAll`, `MinimumCount`). An upstream key a mapper cannot transform produces no downstream run, silently.

```python
from airflow.sdk import Asset, PartitionedAssetTimetable, StartOfDayMapper, dag, task

hourly_sales = Asset("hourly_sales", uri="s3://example-bucket/sales/hourly/")


@dag(schedule=PartitionedAssetTimetable(assets=hourly_sales, default_partition_mapper=StartOfDayMapper()))
def daily_sales():
    @task
    def build(**context):
        print(context["partition_key"], context["partition_date"])

    build()


daily_sales()
```

Operating partitioned Dags:

```bash
# Rerun the runs of one partition (clears and re-queues them)
airflow dags clear daily_sales --partition-key 2026-03-10 --yes

# Backfill a partition-date range: the same flags as any backfill
airflow backfill create --dag-id daily_sales --from-date 2026-03-01 --to-date 2026-03-07 --dry-run
```

> [!WARNING]
> `airflow partitions clear` is not "rerun a partition". It **erases** the `partition_key` and `partition_date` columns on the selected Dag runs, and with `--clear-task-instances` also re-queues them. Always run it with `--dry-run` first: `airflow partitions clear -d daily_sales -k 2026-03-10 --dry-run`. For reruns use `airflow dags clear`, which for partitioned Dags selects by `--run-id`, `--partition-key`, or `--partition-date-start` / `--partition-date-end`.

---

## 8. Event-driven scheduling with asset watchers

An `AssetWatcher` runs a trigger in the **triggerer**; every event the trigger yields becomes an asset event, which schedules consumers. No sensor task, no worker slot.

```python
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

incoming = Asset(
    "incoming_file",
    watchers=[AssetWatcher(name="incoming_file_watcher", trigger=FileDeleteTrigger(filepath="/data/incoming/ready"))],
)


@dag(schedule=incoming, catchup=False)
def on_file():
    @task
    def handle(**context):
        print(context["triggering_asset_events"])

    handle()


on_file()
```

* The trigger must subclass `BaseEventTrigger` (validated in `AssetWatcher`). Core 3.4 ships with the standard provider's `FileDeleteTrigger` and `DirectoryFileDeleteTrigger`; message queues use `MessageQueueTrigger` from the Common Messaging provider with Kafka, SQS, Redis and other providers. Provider versions are independent of core.
* `FileDeleteTrigger` deletes the file when it fires. That is how it avoids firing twice; do not point it at data you need.
* The triggerer must be running and able to reach the source. Watchers are registered when the Dag is parsed and unpaused *(not verified against source)*.

---

## 9. Multi-team deployments and asset access

With `[core] multi_team = True`, an asset can declare who may produce and consume it: `Asset(..., access_control=AssetAccessControl(producer_teams=["sales"], consumer_teams=["finance"], allow_global=False))`. It scopes cross-team asset scheduling; it does not isolate task execution. See [security hardening](../05_deployment_and_config/03_security_hardening.md).

---

## 10. "Why didn't my Dag run?" checklist

| Check | How |
| :--- | :--- |
| Paused or draining? | UI, or `airflow dags list` |
| Import error? | `airflow dags list-import-errors` |
| Schedule is `None` (the default)? | `airflow dags details <dag_id>` |
| Waiting for the end of a data interval? | Data-interval timetables run one period late by design (§2) |
| `max_active_runs` reached? | Running and queued runs in the UI |
| Asset list is an AND? | `schedule=[a, b]` needs both (§1) |
| Partitioned event, non-partitioned consumer? | Partitioned events do not trigger it (§7) |
| Before `start_date` or after `end_date`? | `airflow dags next-execution <dag_id>` |

More symptoms and fixes are in the [troubleshooting playbook](../06_operations_and_housekeeping/05_troubleshooting_playbook.md).
