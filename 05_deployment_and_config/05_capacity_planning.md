# Capacity Planning

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

The official docs explain each tuning option on its own. This page connects them into sizing arithmetic for Airflow 3.4: how many task slots you really have, whether the Dag processor keeps up, how many database connections the deployment opens, and how big the triggerer and API server tier must be. It ends with a worked example.

> [!IMPORTANT]
> **Every number on this page that is not a config default is illustrative.** Parse times, task rates and connection counts depend on your Dags, executor, database and hardware. Use the formulas with your own measurements (metrics in §9), then load-test on staging.

---

## 1. The Concurrency Funnel

A task runs only when **every** limit below has room. The effective ceiling is the smallest one.

| Limit | Key / source | 3.4 default | Scope |
| :--- | :--- | :--- | :--- |
| Scheduler parallelism | `[core] parallelism` | `32` | Per scheduler. Total = value × schedulers. |
| Pool slots | `default_pool` (created with `[core] default_pool_task_slot_count`) | `128` | Global, per pool. The key only applies when the pool is first created; change an existing pool with `airflow pools set` or the API. |
| Per-Dag-run tasks | `[core] max_active_tasks_per_dag` / Dag `max_active_tasks` | `16` | Per Dag run |
| Active runs | `[core] max_active_runs_per_dag` / Dag `max_active_runs` | `16` | Per Dag |
| Worker slots (Celery) | `[celery] worker_concurrency` × workers | `16` per worker | Per queue |
| Worker slots (Kubernetes) | Cluster quota / node capacity | n/a | Per namespace |
| Per-task limits | `max_active_tis_per_dag`, `pool_slots` on the task | unset / `1` | Per task |

```text
running tasks ≤ min( parallelism × schedulers,
                     sum of pool slots for the pools in use,
                     total worker slots for the queues in use,
                     per-Dag limits summed over active Dags )
```

Deferred tasks free their worker slot. They keep occupying a pool slot only if the pool has `include_deferred` set (`--include-deferred` on `airflow pools set`); it is off by default. See [Concurrency, Pools and Priority](../03_task_execution/03_concurrency_pools_and_priority.md).

> [!TIP]
> The most common surprise: raising `[celery] worker_concurrency` and adding workers changes nothing, because `[core] parallelism` (32 per scheduler) or `default_pool` (128) is the real cap. Watch `executor.open_slots` and `pool.open_slots.<pool>` together.

---

## 2. Scheduler Throughput

Scheduler throughput is bounded by how fast one loop can create runs, examine task instances and hand them to the executor, all under a database lock (the "critical section").

| Knob | Default | Raise when | Risk |
| :--- | :--- | :--- | :--- |
| `[scheduler] max_tis_per_query` | `16` | Many tasks are `scheduled` while executor slots are free | Longer critical section and lock contention |
| `[scheduler] max_dagruns_to_create_per_loop` | `10` | Many Dags due at the same minute | Longer loops |
| `[scheduler] max_dagruns_per_loop_to_schedule` | `20` | Many concurrently running Dag runs | Longer loops |
| `[core] parallelism` | `32` | Executor slots exist but the scheduler stops queuing | More concurrent DB writes |
| Number of schedulers | 1 | One loop is saturated (`scheduler.scheduler_loop_duration` high, CPU at 100%) | `scheduler.critical_section_busy` rises: schedulers wait on each other |

Rule of thumb (illustrative): add a second scheduler for availability first. Add a third only if the loop duration is high **and** `scheduler.critical_section_busy` stays low. If the critical section is busy, the database is the bottleneck, and more schedulers make it worse.

---

## 3. Dag Processor Parsing Capacity

One Dag processor runs `[dag_processor] parsing_processes` (default `2`) parser processes. Each file is re-parsed at most every `[dag_processor] min_file_process_interval` (default `30` s); new files are discovered on each bundle refresh (`[dag_processor] refresh_interval`, default `300` s).

```text
cycle_time ≈ (number_of_files × average_parse_seconds) / (parsing_processes × dag_processors_on_that_bundle)

healthy when  cycle_time  <  min_file_process_interval
```

If `cycle_time` is larger than `min_file_process_interval`, every file is parsed less often than configured, Dag changes show up late, and the processor runs at 100% CPU forever.

| Example (illustrative) | Files | Avg parse | Processes | Cycle | Verdict for 30 s interval |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Small | 100 | 0.5 s | 2 | 25 s | OK, tight |
| Medium | 400 | 0.8 s | 2 | 160 s | Behind: raise processes or interval |
| Medium, tuned | 400 | 0.8 s | 8 | 40 s | Set `min_file_process_interval` to `60` |
| Heavy top-level code | 400 | 4 s | 8 | 200 s | Fix the Dags first (top-level imports, API calls) |

How to measure:

* `dag_processing.total_parse_time` (gauge): seconds to parse the last full queue; compare with `min_file_process_interval`.
* `dag_processing.last_duration` (per file): find the slow files.
* `dag_processing.file_path_queue_size`: files waiting; it should drain every cycle.
* `dag_processing.processor_timeouts`: files killed by `[dag_processor] dag_file_processor_timeout` (`50` s).
* The processor log prints a stats table every `[dag_processor] print_stats_interval` (`30` s).

Sizing: each parser process is one CPU-bound Python process; give the Dag processor about one core per parsing process plus one for the manager (illustrative). Scale out with more processors, partitioned by `airflow dag-processor --bundle-name`, rather than one huge processor.

---

## 4. Metadata Database Connections

Each Airflow **process** that talks to the database has its own SQLAlchemy pool of up to `[database] sql_alchemy_pool_size` + `[database] sql_alchemy_max_overflow` connections (defaults `5` + `10` = **15**). Parser child processes of the Dag processor and task processes on workers do not use the metadata DB.

| Component | DB-using processes (illustrative) | Max connections at defaults |
| :--- | :--- | :--- |
| API server replica | one per `[api] workers` (uvicorn) worker | 15 × workers, plus async-engine connections *(not verified against source)* |
| Scheduler | 1 | 15 |
| Dag processor | 1 (the manager) | 15 |
| Triggerer | 1 | 15 |
| Celery worker | 0 for the metadata DB; but the Celery result backend opens its own connections, to the metadata DB if `[celery] result_backend` is unset | per worker process |
| Kubernetes task pod | 0 | 0 |

```text
max_connections ≈ 15 × (api_replicas × api_workers + schedulers + dag_processors + triggerers) + result-backend connections
```

Most of these connections sit idle. Put **PgBouncer** in transaction mode between Airflow and PostgreSQL when the sum approaches the server's `max_connections`. In 3.4 the default psycopg 3 driver has server-side prepared statements turned off (`prepare_threshold = None`), so transaction-mode pooling works without extra settings (3.4 release notes). The Helm chart's PgBouncer defaults are `metadataPoolSize: 10`, `resultBackendPoolSize: 5`, `maxClientConn: 100`. See [Metadata Database](../04_infrastructure_and_cloud/04_metadata_database.md).

> [!WARNING]
> Airflow 2 sizing advice counted a connection per running task. In Airflow 3 tasks go through the Execution API, so the **API server** absorbs that load. Size API replicas and their pools for the number of concurrently running tasks.

---

## 5. Worker Capacity

**Celery** (`apache-airflow-providers-celery`; provider versions are independent of core):

| Key | Default | Note |
| :--- | :--- | :--- |
| `[celery] worker_concurrency` | `16` | Task processes per worker; override with `airflow celery worker --concurrency`. |
| `[celery] worker_autoscale` | unset | `max,min` processes. |
| `[celery] worker_prefetch_multiplier` | `1` | Keep `1` for long tasks. |

Each running task is a supervisor process plus the task process. Memory per slot is dominated by your task code; measure it, then: `worker_concurrency ≈ (worker RAM − headroom) / peak RAM per task` (illustrative).

**Kubernetes** (`KubernetesExecutor`, `apache-airflow-providers-cncf-kubernetes`): one pod per task. Capacity is the namespace quota and node pool size. `[kubernetes_executor] worker_pods_creation_batch_size` (default `1`) limits how many pods the scheduler creates per loop; raise it when many tasks start at once. Pod start-up latency (image pull, scheduling) adds to every task; prefer Celery for many short tasks.

---

## 6. Triggerer Capacity

One triggerer runs up to `[triggerer] capacity` (`1000`) triggers in one asyncio event loop. The real limit is usually CPU or a blocking trigger, not the count.

* Watch `triggers.running`, `triggerer.capacity_left`, `triggers.blocked_main_thread` and `triggerer.trigger_queue_delay`.
* If `triggers.blocked_main_thread` increases, a trigger is doing blocking I/O; fix the trigger rather than adding capacity.
* For HA run at least two triggerers; keep `[triggerer] max_trigger_to_select_per_loop` (`50`) low so they share the load.

---

## 7. API Server

In Airflow 3 the API server serves the UI, the REST API **and every worker's Execution API calls** (heartbeats every `[workers] min_heartbeat_interval` = 5 s per running task, XCom, variable and connection reads, state changes).

```text
execution_api_rps ≈ running_tasks / 5  +  task starts/finishes per second × (a few calls each)
```

Scale with replicas behind a load balancer; the config description recommends that over raising `[api] workers`. Optionally run dedicated replicas for workers with `airflow api-server --apps execution`, so a UI spike does not slow heartbeats. If the API server is slow, workers time out (`[workers] execution_api_timeout` `5.0` s) and, after `[workers] max_failed_heartbeats` (`30`) misses, kill their tasks.

---

## 8. Worked Example (Illustrative)

Target: 600 Dags in 400 files, peak 300 concurrent tasks, mostly 2–10 minute Python tasks, 150 sensors converted to deferrable mode, CeleryExecutor, PostgreSQL.

| Item | Calculation | Setting |
| :--- | :--- | :--- |
| Schedulers | 2 for HA | 2 |
| Parallelism | 300 / 2 schedulers, plus headroom | `[core] parallelism = 192` |
| Default pool | ≥ 300 | `airflow pools set default_pool 320 "default"` |
| Celery workers | 300 / 16 = 19; with 20% headroom | 24 workers × `worker_concurrency = 16` |
| Parsing | measured 0.8 s avg × 400 files = 320 s of CPU; target cycle 40 s → 8 processes | `[dag_processor] parsing_processes = 4` on 2 processors; `min_file_process_interval = 60` |
| Triggerers | 150 triggers ≪ 1000; 2 for HA | 2 |
| API servers | 300 tasks / 5 s ≈ 60 heartbeat req/s + UI | 3 replicas × 2 workers |
| DB connections | 15 × (3 × 2 + 2 + 2 + 2) = 180 max | PgBouncer in transaction mode; Celery result backend on a separate database |

Validate on staging with a synthetic load (many short Dags) and check §9 before go-live.

---

## 9. What to Watch

| Resource | Saturation signal | Metric |
| :--- | :--- | :--- |
| Scheduler | Loop slow, critical section contended | `scheduler.scheduler_loop_duration`, `scheduler.critical_section_duration`, `scheduler.critical_section_busy` |
| Slots | Tasks waiting on slots | `executor.open_slots`, `pool.open_slots`, `pool.starving_tasks`, `scheduler.tasks.starving` |
| Queues | Tasks queued too long | `task.queued_duration`, `executor.queued_tasks` |
| Parsing | Cycle longer than interval | `dag_processing.total_parse_time`, `dag_processing.file_path_queue_size`, `dag_processing.processor_timeouts` |
| Triggerer | Full or blocked | `triggerer.capacity_left`, `triggers.blocked_main_thread` |
| Start latency | End-to-end scheduling delay | `dagrun.schedule_delay`, `dagrun.first_task_scheduling_delay` |

Exact names and tags are in the [metrics reference](../09_reference/03_metrics_reference.md); dashboards and alerts are in [Monitoring and Observability](../06_operations_and_housekeeping/03_monitoring_and_observability.md).
