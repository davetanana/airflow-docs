# Docker Images

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

**Official docs:** [Docker image: Docker Image for Apache Airflow](https://airflow.apache.org/docs/docker-stack/index.html) · [Docker image: Building the image](https://airflow.apache.org/docs/docker-stack/build.html) · [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/3.4.0/howto/docker-compose/index.html)

The official "Docker Image for Apache Airflow" docs (`docker-stack-docs/` in the source) describe every build argument. This page covers the decisions an operator actually has to make: which published image to start from, when to extend and when to customize, how to add packages without silently moving Airflow, what the entrypoint does on its own (including one behavior that hides failed migrations), and how to pin tags so a rebuild is reproducible.

Sources: the `Dockerfile` and its embedded entrypoint script (`entrypoint_prod.sh`, copied to `/entrypoint` in the image) as found in the repository with `ARG AIRFLOW_VERSION="3.4.0"`, and `docker-stack-docs/`. The Dockerfile is not versioned with SemVer, so check the image changelog (`docker-stack-docs/changelog.rst`) when you move to a new Airflow patch release.

---

## 1. What Is Published

Images go to `apache/airflow` on Docker Hub for every release and every supported Python version, as multi-platform (amd64/arm64) images.

| Tag pattern | Contents | Use for |
| :--- | :--- | :--- |
| `apache/airflow:3.4.0-python3.12` | **Regular** image: Airflow plus a large default set of extras/providers (Amazon, Google, Azure, Celery, cncf.kubernetes, FAB, Postgres, MySQL, Redis, SSH, ...) | Trying things out; extending when you need many of those providers |
| `apache/airflow:slim-3.4.0-python3.12` | **Slim** image: core Airflow only, database clients installed, **no database provider** | Recommended base for production images |
| `apache/airflow:3.4.0` / `slim-3.4.0` | Same, with the default Python version for that release | Avoid: the default Python can change between releases |
| `apache/airflow:latest`, `slim-latest`, `latest-pythonX.Y` | Newest released Airflow | Never in production |

> [!WARNING]
> The slim image has no `apache-airflow-providers-postgres` (or MySQL) provider, so it cannot talk to a Postgres metadata database until you add the provider. It also has no `fab` provider, while the Helm chart sets the FAB auth manager by default (see [Kubernetes and the Helm chart](03_kubernetes_and_helm_chart.md#6-secrets-and-auth)).

### The 3.4 base image change

Starting with 3.4.0, images are built on the Docker Hardened Image for Python on **Debian 13 "trixie"** (mirrored to `ghcr.io/apache/airflow/base/python`), not on `debian:bookworm-slim` with Python compiled by the build. Things that can break a custom image built on top (from `docker-stack-docs/build-advanced.rst`):

| Change | What can break |
| :--- | :--- |
| Minimal OS: no compiler, `curl`, `wget`, `git`, `gzip`, `which`, `ldconfig`, no `dash` | `RUN` steps that assumed those tools exist |
| Python installed as Debian packages under `/usr` and held with `apt-mark hold` | `apt-get install` of something that depends on Debian's `python3` (for example NodeSource `nodejs`) fails instead of replacing Python |
| Extra apt repository `dhi.io/deb/debian/main`, packages with a `+dhi<N>` suffix | Package versions differ from stock Debian |
| GCC 14 and `-Werror=implicit-function-declaration` | Old source distributions that used to build with warnings fail to compile |
| Patched CPython (for example stricter `pkgutil.get_data()`) and no PGO build | Rare library failures; CPU-bound Python can be slower |

A `legacy` flavor (the pre-3.4 build) can still be built from the 3.4.x `Dockerfile` with `--build-arg AIRFLOW_IMAGE_FLAVOR="legacy"`. It is deprecated and scheduled for removal in 3.5.0. The docs disagree about whether legacy images are published for 3.4.x (`legacy-images.rst` says no, `build-arg-ref.rst` says yes); check Docker Hub before relying on one *(not verified against source)*.

---

## 2. Extend or Customize?

| Question | Answer |
| :--- | :--- |
| Adding providers, pure-Python packages, Dags or config files? | **Extend the slim image** (§3). Builds in seconds, needs no Airflow sources |
| Need many of the regular image's providers, or not sure yet? | Extend the regular image. About twice the size |
| A package fails with `gcc` / `pg_config` / Rust compiler errors? | **Customize**: build from the `Dockerfile` with `AIRFLOW_EXTRAS`, `ADDITIONAL_PYTHON_DEPS`, `ADDITIONAL_DEV_APT_DEPS`, `ADDITIONAL_RUNTIME_APT_DEPS`. Compilers stay in the build stage only |
| Need a forked Airflow, or to build from vetted sources? | Customize |

Installing `build-essential` while extending works, but leaves hundreds of megabytes of toolchain in every image.

---

## 3. Extending Safely

The image runs as user `airflow` (UID 50000, GID 0) with Python packages in the user's `~/.local` virtualenv. The image ships `AIRFLOW_VERSION` as an environment variable and the constraints it was built with in `${HOME}/constraints.txt` (the no-providers set in the slim image).

```dockerfile
FROM apache/airflow:slim-3.4.0-python3.12

# pip must run as the airflow user; /root/bin/pip blocks installs as root
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-postgres" \
      "apache-airflow-providers-cncf-kubernetes" \
      "apache-airflow-providers-fab" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

COPY --chown=airflow:0 requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
```

The two rules that matter:

1. **Always put `apache-airflow==${AIRFLOW_VERSION}` in the same `pip install`.** Without it, pip may upgrade or downgrade Airflow to satisfy one of your packages, and the image silently runs a different Airflow than its tag. With it, a conflict fails the build.
2. **Use the constraints file for providers released with this Airflow.** Leave it out only for a provider you deliberately want newer than the release, and then pin that provider with `==` instead.

Pin every line of `requirements.txt` with `==`, and rebuild regularly to pick up security fixes.

### apt packages

```dockerfile
FROM apache/airflow:slim-3.4.0-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends unixodbc \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
```

Check with `apt-get install --simulate <package>` first: on the trixie hardened base, a package that depends on `python3` pulls in a second interpreter or fails on the held Python packages.

### Directories your containers write to

The image is OpenShift-style: it must work for **any UID as long as the GID is 0**. When you create directories, make them group-writable (`umask 0002` in the same `RUN`, or `chmod g+rwX`) and owned by group `0`.

---

## 4. `AIRFLOW_UID`, User 50000 and GID 0

| Fact | Source |
| :--- | :--- |
| Default user: `airflow`, UID `50000`, primary GID `0` | `ARG AIRFLOW_UID="50000"`, `adduser ... --uid "${AIRFLOW_UID}" --gid "0"` in the `Dockerfile` |
| Running with UID 50000 and a non-zero GID | Entrypoint prints a warning and continues |
| Running with **any other UID and a non-zero GID** | Entrypoint prints `ERROR! You should run the image with GID=0` and **exits 1** |
| Running with an arbitrary UID and GID 0 | Supported. The entrypoint adds a passwd entry if `/etc/passwd` is writable and sets `HOME=/home/airflow` |
| Running as root (UID 0) | Works, with a warning; the entrypoint adds the airflow user's `site-packages` to `PYTHONPATH` |

`AIRFLOW_UID` as an environment variable is a convention of the official `docker-compose.yaml` (`user: "${AIRFLOW_UID:-50000}:0"`): set it to your host UID so bind-mounted `dags/`, `logs/`, `plugins/` and `config/` are owned by you. The image itself reads it only as a build argument.

```bash
echo "AIRFLOW_UID=$(id -u)" > .env     # for docker compose on Linux hosts
```

---

## 5. What the Entrypoint Does

The image's `ENTRYPOINT` is `["/usr/bin/dumb-init", "--", "/entrypoint"]` and `CMD` is empty. In order, `/entrypoint`:

1. Checks UID/GID (§4) and sets `umask 0002`.
2. If the command is `scheduler`, `dag-processor`, `triggerer` or `api-server` (with or without a leading `airflow`), waits for the database by running `airflow db check` up to `CONNECTION_CHECK_MAX_COUNT` times (default `20`), `CONNECTION_CHECK_SLEEP_TIME` seconds apart (default `3`). Set `CONNECTION_CHECK_MAX_COUNT=0` to skip.
3. If `_AIRFLOW_DB_MIGRATE` is non-empty, runs `airflow db migrate`. (`_AIRFLOW_DB_UPGRADE` still works but logs a deprecation warning.)
4. If `_AIRFLOW_WWW_USER_CREATE` is non-empty, creates an admin user from `_AIRFLOW_WWW_USER_USERNAME` (default `admin`), `_AIRFLOW_WWW_USER_PASSWORD` or `_AIRFLOW_WWW_USER_PASSWORD_CMD`, and `_FIRSTNAME`/`_LASTNAME`/`_EMAIL`/`_ROLE` variants. **Only when `[core] auth_manager` is the FAB auth manager**; otherwise it prints "Skipping user creation".
5. If `_PIP_ADDITIONAL_REQUIREMENTS` is set, `pip install`s it at every container start, with a loud "NEVER use it in production" warning.
6. If the first argument is `bash` or `python`, `exec`s it.
7. For `scheduler` or `celery` commands with `[core] executor` set to `CeleryExecutor`, waits for the Celery broker port.
8. If there are no arguments and `_AIRFLOW_DB_MIGRATE=true`, exits 0 (a "migrate-only" container).
9. Otherwise `exec airflow "$@"`.

> [!WARNING]
> **`_AIRFLOW_DB_MIGRATE` swallows migration failures.** The entrypoint runs `airflow db migrate || true`. If the migration fails, the container continues to start the command anyway. Run migrations as a separate step (a Kubernetes Job, a CI step, or `docker run ... airflow db migrate`) whose exit code you check, and don't rely on the entrypoint variable in production. See [Metadata database §6](04_metadata_database.md#6-schema-migrations).

> [!WARNING]
> **`_PIP_ADDITIONAL_REQUIREMENTS` is for experiments.** Every restart re-resolves packages from the internet without constraints, so two containers of the "same" deployment can run different library versions, and a PyPI outage stops your containers from starting.

### Signals

`DUMB_INIT_SETSID` defaults to `1`: `dumb-init` forwards signals to the whole process group. Set it to `0` when the container runs a supervisor that must handle shutdown itself, such as a Celery worker doing a warm shutdown, so that only the direct child receives `SIGTERM`. If you replace the entrypoint, keep `dumb-init` as PID 1.

---

## 6. Configuration and Secrets in Containers

* Every config key can be set as `AIRFLOW__SECTION__KEY`; secrets can come from `AIRFLOW__SECTION__KEY_CMD` or `AIRFLOW__SECTION__KEY_SECRET` (secrets backend) for sensitive keys. See [Configuration and tuning](../05_deployment_and_config/01_configuration_and_tuning.md).
* The image sets `AIRFLOW__CORE__LOAD_EXAMPLES=false`.
* **Set `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API_AUTH__JWT_SECRET` and `AIRFLOW__API__SECRET_KEY` explicitly, identical in every container.** A container that starts without an `airflow.cfg` generates its own random Fernet key and JWT secret on first run (see [Installation §9](01_installation_and_python.md#9-first-run-traps)).
* Workers and the Dag processor reach the API server through `[core] execution_api_server_url` (`AIRFLOW__CORE__EXECUTION_API_SERVER_URL`, for example `http://airflow-apiserver:8080/execution/` in the official compose file).
* Don't bake secrets into image layers. `docker history` shows them.

---

## 7. Dags in the Image or Not

| Approach | Pros | Cons |
| :--- | :--- | :--- |
| `COPY dags/ /opt/airflow/dags/` | Immutable, versioned with the image; what runs is exactly what you tested | Every Dag change is an image build and a rollout of all components |
| Volume (bind mount, PVC, git-sync sidecar) | Fast Dag changes | Code and image drift apart; needs shared storage or a sidecar |
| Dag bundles (`[dag_processor] dag_bundle_config_list`, for example `GitDagBundle` from the `git` provider) | Versioned Dag code fetched by Airflow itself; tasks run the bundle version their Dag run started with | Needs the `git` provider and credentials (a connection) on every component that runs tasks |

Keep Python dependencies in the image either way: a Dag bundle can ship Python files, but not new installed packages.

---

## 8. Pinning and Rebuilding

1. Pin `FROM` to a full version and Python tag, for example `apache/airflow:slim-3.4.0-python3.12`. For byte-for-byte reproducibility, pin the digest too: `FROM apache/airflow:slim-3.4.0-python3.12@sha256:<digest>` (get it with `docker buildx imagetools inspect apache/airflow:slim-3.4.0-python3.12`).
2. Tag your own image with something traceable, such as `<registry>/airflow:3.4.0-<git_sha>`, never `latest`.
3. Rebuild on a schedule for OS security fixes. A published tag can be republished with a newer base for CVE fixes *(not verified against source)*, which is a second reason to pin digests.
4. Use the same image for every component (scheduler, API server, Dag processor, triggerer, workers). Mixed images are the classic source of "works in one task, fails in another" bugs and of serialization mismatches.

```bash
docker build . --pull --tag registry.example.internal/airflow:3.4.0-abc1234
docker run --rm registry.example.internal/airflow:3.4.0-abc1234 airflow version
docker run --rm registry.example.internal/airflow:3.4.0-abc1234 airflow providers list
docker run --rm registry.example.internal/airflow:3.4.0-abc1234 bash -c "pip check"
```

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
| :--- | :--- | :--- |
| `ERROR! You should run the image with GID=0` and exit | Container started with `--user <uid>:<gid>` where gid is not 0 | Use `<uid>:0` |
| `pip install` fails as root with a protection message | `/root/bin/pip` wrapper refuses root installs | Switch to `USER airflow` for pip |
| `error: command 'gcc' failed` / `pg_config executable not found` | Package has no wheel for this Python/arch | Pick a version with wheels or customize the image (§2) |
| Component logs "Database migration required" and exits | Schema older than the code | Run `airflow db migrate` as a separate, checked step |
| Connections fail with `InvalidToken` after a redeploy | Each container generated its own Fernet key | Set `AIRFLOW__CORE__FERNET_KEY` explicitly (§6) |
| `ModuleNotFoundError: airflow.providers.postgres` with the slim image | No database provider in slim | Add `apache-airflow-providers-postgres` |

---

## See Also

* [Installation and Python](01_installation_and_python.md): constraints and extras explained.
* [Kubernetes and the Helm chart](03_kubernetes_and_helm_chart.md)
* [Deployment runbook](../05_deployment_and_config/02_deployment_runbook.md)
* [Security hardening](../05_deployment_and_config/03_security_hardening.md)
