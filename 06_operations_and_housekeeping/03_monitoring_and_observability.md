# Monitoring and Observability: Metrics, Health Checks, Traces and Deadline Alerts

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

Airflow emits well over a hundred metrics. This page picks the ones worth an alert, says what threshold to start with, and covers the pieces around them: which metrics backend wins when several are enabled, why your per-Dag tags may silently disappear, the health endpoints for each component, OpenTelemetry tracing, and deadline alerts, the Airflow 3 replacement for SLAs. Metric names are from `shared/observability/src/airflow_shared/observability/metrics/metrics_template.yaml` on `v3-4-stable`; every metric is listed in the [metrics reference](../09_reference/03_metrics_reference.md).

---

## 1. Choosing and Enabling a Metrics Backend

Airflow sends metrics to **one** backend per process. `get_stats_factory()` in `airflow/observability/metrics/stats_utils.py` checks in this order and uses the first that is on:

| Order | Setting | Backend |
| :--- | :--- | :--- |
| 1 | `[metrics] statsd_datadog_enabled = True` | DogStatsD (tags supported natively) |
| 2 | `[metrics] statsd_on = True` | Plain StatsD (`statsd_host` `localhost`, `statsd_port` `8125`, `statsd_prefix` `airflow`) |
| 3 | `[metrics] otel_on = True` | OpenTelemetry (OTLP) |
| — | none | No metrics |

> [!WARNING]
> Turning on `otel_on` while `statsd_on` is still `True` gives you StatsD only, with no error. The Helm chart handles this for you: `otelCollector.metricsEnabled: true` renders `statsd_on = False` and `otel_on = True`.

**StatsD (the chart default).** The chart deploys a `statsd-exporter` (`statsd.enabled: true`) and points `[metrics] statsd_host` at it on port 9125; Prometheus scrapes the exporter. Mappings live in `chart/files/statsd-mappings.yml`.

**OpenTelemetry.** Since 3.2 the `[metrics] otel_host`, `otel_port`, `otel_interval_milliseconds`, `otel_service`, `otel_ssl_active` and `otel_debugging_on` keys are deprecated. Configure the exporter with the standard OpenTelemetry environment variables instead:

```bash
export AIRFLOW__METRICS__OTEL_ON=True
export OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.0.20:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_SERVICE_NAME=airflow
export OTEL_METRIC_EXPORT_INTERVAL=30000
```

Set them on every component, including workers: task-level metrics are emitted where the task runs.

---

## 2. Names, Tags and Cardinality

Each metric has a **new name** with variables as tags (`ti.finish` with `dag_id`, `task_id`, `state` tags) and often a **legacy name** with the variables baked in (`ti.finish.<dag_id>.<task_id>.<state>`). `[metrics] legacy_names_on` (default `True`) emits both.

> [!IMPORTANT]
> **Plain StatsD drops tags.** The StatsD logger only appends tags to the name when InfluxDB-style tags are enabled (`[metrics] statsd_influxdb_enabled`); otherwise they are discarded (`statsd_logger.py`). So on plain StatsD the legacy names are your only per-Dag detail. Do not turn `legacy_names_on` off unless you use DogStatsD, InfluxDB tags or OpenTelemetry.

Filtering, all comma-separated, matched as **regular expressions anywhere in the lowercased name** (`re.findall` in `validators.py`):

| Key | Default | Use |
| :--- | :--- | :--- |
| `[metrics] metrics_allow_list` | empty (all) | Send only matching metrics, e.g. `^scheduler,^executor,^pool,heartbeat` |
| `[metrics] metrics_block_list` | empty | Drop matching metrics; ignored when an allow list is set |
| `[metrics] statsd_disabled_tags` | `job_id,run_id` | Tag keys never sent (keep `run_id` here: it is unbounded) |
| `[metrics] dag_tags_in_metrics` | `False` | Add Dag tags as metric tags. Free-form strings, so watch cardinality |
| `[metrics] stat_name_handler` | empty | Your function to rewrite or validate names |

Per-task legacy names (`dag.<dag_id>.<task_id>.duration`, `ti.queued.<queue>.<dag_id>.<task_id>`) multiply with every task. On a large deployment, block them at the exporter or with `metrics_block_list` and keep the aggregate ones.

---

## 3. The Metrics That Matter, and What to Alert On

Thresholds are illustrative starting points; tune them to your baseline. Names below are without the `airflow` prefix that StatsD adds.

### Is it alive?

| Metric | Type | Alert when |
| :--- | :--- | :--- |
| `scheduler_heartbeat` | counter | Rate is 0 for 1 minute (every scheduler increments it on each heartbeat, default every 5 s) |
| `triggerer_heartbeat` | counter | Rate is 0 for 1 minute, if you use deferrable operators |
| `dag_processor_heartbeat` | counter | Rate is 0 for 1 minute |
| `{job_name}_heartbeat_failure` | counter | Any increase: a job could not write its heartbeat (an `OperationalError`, i.e. DB trouble). The template's examples suggest `SchedulerJob_...`, but `jobs/job.py` builds the name from the `Job` class itself, so in 3.4 the emitted name is `job_heartbeat_failure` for every job type |

### Is the scheduler keeping up?

| Metric | Type | Alert when |
| :--- | :--- | :--- |
| `scheduler.scheduler_loop_duration` | timer | p95 above ~10 s, or a sustained climb |
| `scheduler.critical_section_duration` | timer | p95 above a few seconds: the task-queuing query is slow |
| `scheduler.critical_section_busy` | counter | High rate with several schedulers: they are waiting on each other's lock |
| `scheduler.tasks.starving` | gauge | Above 0 for 10+ minutes: tasks blocked by pool or concurrency limits |
| `dagrun.schedule_delay` | timer | Above a few minutes: runs start late |
| `dagrun.first_task_scheduling_delay` | timer | Rising: scheduler or executor saturated |
| `task.queued_duration` | timer | p95 above a few minutes: tasks wait for workers |

### Is there capacity?

| Metric | Type | Alert when |
| :--- | :--- | :--- |
| `executor.open_slots` | gauge | 0 for 10+ minutes: `[core] parallelism` or executor capacity is the bottleneck |
| `executor.queued_tasks` | gauge | Growing while `executor.running_tasks` is flat: workers are not picking up |
| `pool.starving_tasks` (tag `pool_name`) | gauge | Above 0 for 10+ minutes on a pool |
| `pool.open_slots` | gauge | 0 on `default_pool` for long periods |
| `triggerer.capacity_left` | gauge | Below 10% of `[triggerer] capacity` (1000) |

### Are tasks dying abnormally?

| Metric | Type | Alert when |
| :--- | :--- | :--- |
| `task_instances_without_heartbeats_killed` | counter | Any increase: tasks lost their worker (OOM, node loss, network). See [Troubleshooting §4](05_troubleshooting_playbook.md#4-task-failed-without-a-heartbeat-zombies) |
| `scheduler.tasks.killed_externally` | counter | Any increase: the executor reported an end state the task never recorded |
| `ti_failures` | counter | Rate above your baseline |
| `triggers.blocked_main_thread` | counter | Any sustained rate: a trigger is doing blocking I/O in the event loop |
| `triggers.failed` | counter | Any increase |

### Are Dags healthy?

| Metric | Type | Alert when |
| :--- | :--- | :--- |
| `dag_processing.import_errors` | gauge | Above 0 |
| `dag_processing.processor_timeouts` | counter | Any increase: a file exceeds `[dag_processor] dag_file_processor_timeout` |
| `dag_processing.total_parse_time` | gauge | Above `[dag_processor] min_file_process_interval`: parsing cannot keep up |
| `dag_processing.file_path_queue_size` | gauge | Growing steadily |
| `dag.serialization.version_created` | counter | Steady rate outside deploys: Dag version inflation |
| `deadline_alerts.deadline_missed` | counter | Any increase, if you rely on deadlines (see [§6](#6-deadline-alerts-replace-slas)) |

---

## 4. Health Endpoints

| Component | How to check | Notes |
| :--- | :--- | :--- |
| API server and overall | `GET /api/v2/monitor/health` | JSON with `metadatabase`, `scheduler`, `triggerer`, `dag_processor`; unauthenticated (mounted outside the authenticated router, like `/api/v2/version`). Used by the chart's API server probes |
| Execution API (for workers) | `GET /execution/health` | Returns `{"status": "healthy"}`; `/execution/health/ping` also pings dependencies and returns 500 if one fails |
| Scheduler (HTTP) | `GET http://<scheduler>:8974/health` | Only when `[scheduler] enable_health_check = True` (default `False`). Port `[scheduler] scheduler_health_check_server_port`. 200 if this host's latest scheduler job is alive, 503 otherwise |
| Scheduler, triggerer, Dag processor (CLI) | `airflow jobs check --job-type <type> --local` | What the chart uses for their liveness probes. See [CLI guide §2](01_cli_operations_guide.md#2-health-is-each-component-alive) |

```bash
curl -s http://10.0.0.10:8080/api/v2/monitor/health | jq .
```

```json
{
  "metadatabase": {"status": "healthy"},
  "scheduler": {"status": "healthy", "latest_scheduler_heartbeat": "2026-11-23T10:15:02+00:00",
                "detailed_status": "healthy", "instances": [{"hostname": "scheduler-0", "latest_scheduler_heartbeat": "..."}]},
  "triggerer": {"status": "healthy", "detailed_status": "healthy", "...": "..."},
  "dag_processor": {"status": "healthy", "detailed_status": "degraded", "...": "..."}
}
```

`status` is `healthy` if **any** instance is alive. `detailed_status` (`healthy` / `degraded` / `down`, from `api/common/airflow_health.py`) compares live instances with the declared work: for Dag processors, which configured bundles are covered; for triggerers in multi-team mode, which teams are covered. A Dag processor started with `--bundle-name` for only some bundles shows `degraded` when the processor for the others is down. Alert on `detailed_status`, not only on `status`.

Liveness uses the job heartbeat and the thresholds `[scheduler] scheduler_health_check_threshold`, `[triggerer] triggerer_health_check_threshold` and `[dag_processor] health_check_threshold` (all 30 s). A triggerer whose subprocess event loop stops responding for `[triggerer] runner_health_check_threshold` (30 s) deliberately skips its heartbeat so that it shows up as unhealthy.

> [!TIP]
> Do not point an external uptime check at `/api/v2/monitor/health` alone and call it "Airflow is up". It reads the `job` table, so it is green as long as the DB is reachable and components heartbeat; it says nothing about whether workers are picking up tasks. Pair it with `executor.open_slots` and `task.queued_duration`.

---

## 5. Traces (OpenTelemetry)

`[traces] otel_on = True` (default `False`) exports spans for Dag runs and task instances through OTLP. As with metrics, the `[traces] otel_host`, `otel_port`, `otel_service`, `otel_ssl_active` and `otel_debugging_on` keys are deprecated since 3.2; use `OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, which takes precedence), `OTEL_SERVICE_NAME` and the other standard variables. `[traces] otel_debug_traces_on` is no longer used.

Related keys:
* `[traces] task_runner_flush_timeout_milliseconds` (30000): how long the task process waits to flush spans on exit.
* `[execution_api] otel_trace_propagation` (`only-authenticated`): whether trace context headers from workers are honored by the Execution API.

In the chart, `otelCollector.tracesEnabled: true` deploys a collector and sets `[traces] otel_on`.

---

## 6. Deadline Alerts Replace SLAs

SLAs are gone in Airflow 3. The `sla_miss_callback` argument still exists on `DAG` only to give a clear error; its docstring says the feature was removed in 3.0 and replaced by deadline alerts. Use `DeadlineAlert` from `airflow.sdk`:

```python
from datetime import timedelta

import pendulum

from airflow.sdk import DAG, AsyncCallback, DeadlineAlert, DeadlineReference, task


async def page_on_call(context: dict, **kwargs):
    # Runs in the triggerer. Must be a top-level async function importable there.
    dag_run = context.get("dag_run")
    print(f"Deadline missed: {dag_run} team={kwargs.get('team')}")


with DAG(
    dag_id="daily_sales",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=2),
        callback=AsyncCallback(page_on_call, kwargs={"team": "data-eng"}),
        name="sales_2h",
    ),
):

    @task
    def load():
        pass

    load()
```

| Reference | Deadline is measured from |
| :--- | :--- |
| `DeadlineReference.DAGRUN_QUEUED_AT` | When the run was queued |
| `DeadlineReference.DAGRUN_LOGICAL_DATE` | The run's logical date |
| `DeadlineReference.FIXED_DATETIME(<datetime>)` | A fixed time |
| `DeadlineReference.AVERAGE_RUNTIME(max_runs=..., min_runs=...)` | Historic average duration |

* `AsyncCallback` runs in the **triggerer** (so you need one running); `SyncCallback` runs on an executor (`executor=` picks which). Both get the context plus your `kwargs`; `context` is a reserved kwarg name.
* Missed deadlines show in the UI under **Deadlines** and increment `deadline_alerts.deadline_missed`; creation failures increment `deadline_alerts.deadline_creation_failed`.
* There is an example Dag in the source: `airflow-core/src/airflow/example_dags/example_deadline_alert.py`.

---

## 7. Listeners: Pushing Events Out

Listeners are plugin hooks (registered through a plugin's `listeners` attribute) for state changes. Use them to feed an event bus or lineage system rather than polling the API.

| Hook | Fires in |
| :--- | :--- |
| `on_task_instance_running`, `on_task_instance_success`, `on_task_instance_failed`, `on_task_instance_skipped` | The task process on the **worker** (Task SDK `task_runner`), so the plugin must be installed on workers |
| `on_dag_run_running`, `on_dag_run_success`, `on_dag_run_failed` | The scheduler, and the API server for runs changed through the API |
| `on_asset_created`, `on_asset_alias_created`, `on_asset_changed`, `on_asset_event_emitted` | Where the asset change happens |
| `on_new_dag_import_error`, `on_existing_dag_import_error` | The Dag processor |

The source ships an example at `airflow-core/src/airflow/example_dags/plugins/event_listener.py`. Keep listener code fast and exception-safe: it runs inline with the event.

---

## 8. UI Views for Operators

| View (path) | Use it for |
| :--- | :--- |
| Home | Import errors, health of scheduler/triggerer/Dag processor, failed runs at a glance |
| Browse > Jobs (`/jobs`) | Every scheduler, triggerer and Dag processor job with hostname and last heartbeat |
| Browse > Audit Log (`/events`) | Who triggered, cleared, paused or edited what; also `heartbeat timeout` and `stuck in queued reschedule` events |
| Dag Runs / Task Instances (`/dag_runs`, `/task_instances`) | Filter by state across all Dags, e.g. every `queued` task instance |
| Admin > Pools (`/pools`) | Running, queued, scheduled and deferred slots per pool |
| Deadlines (`/deadlines`) | Missed and pending deadline alerts |
| Dag Bundles (`/dag_bundles`) | Bundle versions and refresh state |
| Admin > Config (`/configs`) | Effective config, only when `[api] expose_config` is enabled (default `False`) |
| Dag > Backfills | Running backfills, pause or cancel them |

---

**See also:** alert-worthy log lines in the [Troubleshooting playbook](05_troubleshooting_playbook.md), log shipping in [Logging](04_logging.md), and every metric in the [metrics reference](../09_reference/03_metrics_reference.md).
