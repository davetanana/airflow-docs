# Security Hardening

> [!CAUTION]
> Commands on this page can change or delete data and affect running deployments. Try them on a test or staging deployment first, and read the [disclaimer](../README.md#disclaimer). The guide is provided "as is", without warranty.

Airflow's security model (`airflow-core/docs/security/security_model.rst`) and its JWT document (`jwt_token_authentication.rst`) are long and precise. This page turns them into an operator's checklist for 3.4: which keys to set, which auth manager to run, how the two token systems behave, and what Airflow does **not** protect against. Throughout, claims are sorted the way the security model sorts them:

* **Guarantee**: Airflow enforces it in code.
* **Known limitation**: a documented gap; not a bug, and not fixed by configuration.
* **Hardening**: something you, the Deployment Manager, add on top.

---

## 1. The Model in One Table

| Statement | Kind |
| :--- | :--- |
| Workers reach Airflow data only through the Execution API with a short-lived JWT; they never receive DB credentials (if you don't give them any). | Guarantee (worker side) |
| The scheduler and API server never execute Dag-file code. | Guarantee, unless you install plugins or providers that run code there |
| Every `/api/v2` route except `monitor`, `version` and `auth` requires authentication (router-level `Depends(get_user)`). | Guarantee |
| Connection passwords and sensitive extras are masked in the API, UI and `airflowctl`; connection editors can write but not read them. | Guarantee (since 3.0) |
| Dag authors can run arbitrary code on workers, in the Dag processor and in the triggerer. | By design |
| Code running in the Dag processor or triggerer can, if it tries, read the parent's DB credentials and call the Execution API in-process without a token. | Known limitation |
| An Execution API token is not scoped per Dag: a task can read any connection, variable or XCom and trigger or clear any Dag. `ti:self` scope only protects another task instance's own state endpoints. | Known limitation |
| Execution API tokens cannot be revoked and are refreshed while in use. | Known limitation (design) |
| Multi-team mode (`[core] multi_team`) isolates teams in the UI and REST API, and scopes Execution API access to team variables, connections and XComs, but **not** task-level isolation. It is experimental. | Known limitation |
| Separate Unix users, network policies, per-component secrets, asymmetric JWT keys, code review of Dags. | Hardening |

---

## 2. Secrets and Keys That Must Be Set

Three keys have defaults that are **unsafe in multi-process deployments** (see [Configuration §1](01_configuration_and_tuning.md#1-how-a-value-is-resolved)):

| Key | If left to default | Who needs it |
| :--- | :--- | :--- |
| `[core] fernet_key` | Generated only when `airflow.cfg` is first created; if empty, connections and variables are stored **in clear text** | API server, scheduler, Dag processor, triggerer, workers |
| `[api_auth] jwt_secret` | Random per process if empty, so tokens signed by one component fail on another | API server, scheduler only |
| `[api] secret_key` | Random per process, so log fetching from workers and the triggerer fails | API server, scheduler, triggerer, workers |

Generate and distribute them from your secret store:

```bash
# Fernet key (32 url-safe base64 bytes)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# JWT secret and API secret key
openssl rand -base64 48
```

Pass them as environment variables, or as `_CMD` / `_SECRET` variants, which work because all three are marked sensitive:

```bash
export AIRFLOW__CORE__FERNET_KEY_CMD="cat /run/secrets/fernet_key"
export AIRFLOW__API_AUTH__JWT_SECRET_CMD="cat /run/secrets/jwt_secret"
export AIRFLOW__API__SECRET_KEY_CMD="cat /run/secrets/api_secret_key"
```

**Rotating the Fernet key.** `[core] fernet_key` accepts a comma-separated list; the first key encrypts and all keys decrypt (`MultiFernet` in `airflow/models/crypto.py`).

1. Set `fernet_key = <new_key>,<old_key>` on **every** component and restart them.
2. Run `airflow rotate-fernet-key` once. It re-encrypts connections and variables with the first key.
3. Set `fernet_key = <new_key>` and restart again.

> [!WARNING]
> Losing the Fernet key makes every stored connection password and encrypted variable unreadable. Back it up with the database. Don't remove the old key until step 2 has completed.

---

## 3. Authentication: Choose an Auth Manager

| Auth manager | `[core] auth_manager` value | Use |
| :--- | :--- | :--- |
| Simple (default) | `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager` | **Development and testing only.** Users come from `[core] simple_auth_manager_users` (default `admin:admin`, meaning user `admin` with role `admin`); passwords are generated into `simple_auth_manager_passwords.json.generated`. The security model explicitly excludes its weaknesses from security reports. |
| FAB (provider) | `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` | Database users and roles, LDAP / OAuth / OIDC via `webserver_config.py` (`[fab] config_file`). Login rate limiting is on by default (`[fab] auth_rate_limited = True`, `auth_rate_limit = 5 per 40 second`). |
| Keycloak (provider) | `airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager` | Users and authorization in Keycloak; config in `[keycloak_auth_manager]` (`server_url`, `realm`, `client_id`, `client_secret`). |

Notes:

* All three mount their login routes under `/auth`, and each exposes `POST /auth/token` and `POST /auth/token/cli`. OAuth redirect URLs from Airflow 2 need the `/auth` prefix.
* With FAB, set `[fab] cookie_secure = True` when you serve HTTPS (default `False`).
* Provider versions are independent of core; read the provider's changelog before upgrading it.
* `[api_auth] jwt_expiration_time` (24 h) controls UI and REST tokens, `jwt_cli_expiration_time` (1 h) controls CLI tokens. Logout revokes a REST token server-side (the `revoked_token` table). Role changes do not affect a token that was already issued.

---

## 4. Tokens: REST API vs Execution API

| | REST API / UI | Execution API (workers) |
| :--- | :--- | :--- |
| Issued by | API server at login | Scheduler (workload token), API server (execution token at `/run`) |
| Signed with | `[api_auth] jwt_secret` (HS512) or `[api_auth] jwt_private_key_path` (RS256 / EdDSA) | Same keys |
| Audience | `[api_auth] jwt_audience` (unset) | `[execution_api] jwt_audience` = `urn:airflow.apache.org:task` |
| Lifetime | `[api_auth] jwt_expiration_time` = `86400` | Workload token = `[scheduler] task_queued_timeout` (`600`); execution token = `[execution_api] jwt_expiration_time` (`600`), refreshed when under 20% remains |
| Revocable | Yes, on logout | **No** |

**Use asymmetric keys** when you can. With `[api_auth] jwt_private_key_path` set on the token issuers and `[api_auth] trusted_jwks_url` pointing at the public keys, a component that validates tokens cannot forge them. In symmetric mode anyone holding `jwt_secret` can mint a token for any task.

```ini
[api_auth]
jwt_private_key_path = /run/secrets/jwt_ed25519.pem
trusted_jwks_url = /etc/airflow/jwks.json
jwt_issuer = airflow-prod
```

> [!WARNING]
> **A leaked execution token stays valid as long as someone keeps using it.** Refresh reissues a token from its own claims and does not check whether the task has finished (security model, "Execution API tokens not being revocable"). The 10-minute lifetime only bounds a holder who stops calling. Contain workers with network isolation, not with token expiry.

**Edge workers** (`edge3` provider) must hold `[api_auth] jwt_secret` themselves, so any edge host is effectively part of the control plane.

---

## 5. Secrets Masking

* Masked automatically: connection passwords; Variable names and connection extra keys containing any of the default sensitive words (`password`, `secret`, `token`, `api_key`, `private_key`, `authorization`, `passphrase`, `keyfile_dict`, `service_account` and more, see `DEFAULT_SENSITIVE_FIELDS` in `shared/secrets_masker/`).
* Add your own words with `[core] sensitive_var_conn_names` (comma-separated). `[core] hide_sensitive_var_conn_fields` (default `True`) turns the field-name matching on.
* `[logging] min_length_masked_secret` (default `5`): shorter values are never masked, to avoid masking common words.
* Masking works on **known values**. A Dag that prints `os.environ`, or builds a secret from parts, leaks it into task logs. Restrict who can read task logs.
* `airflow info` prints connection strings, including passwords, unless you pass `--anonymize`.

---

## 6. Dag Authors, Multi-Team and Where Code Runs

| Where Dag-author code runs | Isolation you get | Hardening |
| :--- | :--- | :--- |
| LocalExecutor (scheduler host) | None: can affect the scheduler and its config | Use only with fully trusted authors. |
| Celery worker | Shared among all tasks on that worker | Separate queues and workers per trust level; no DB creds on workers. |
| Kubernetes pod | Pod-level | Per-team namespaces, service accounts, network policies. |
| Dag processor | Same Unix user as the parent, which holds DB creds | One processor per team with `--bundle-name`; Unix-user separation; code review. |
| Triggerer | Same as above; shared by all teams unless you run one per team (`--team-name`) | One triggerer per team; code review. |

Other rules from the security model:

* The `plugins` folder and installed packages can run code in the scheduler and API server. Dag authors must **not** have write access to `[core] plugins_folder` or to the image.
* "Trigger Dag" permission does not stop asset-triggered runs: anyone who can emit an asset event can start Dags scheduled on it.
* The audit-log permission shows events for all Dags. Since 3.4, rows not tied to a Dag need the `AUDIT_LOGS_ALL` view (FAB resource `All Audit Logs`).
* Multi-team is experimental in 3.4. Team names must be lower case without `__` (`airflow teams create`, `airflow teams verify`).

---

## 7. Network, TLS and the API Server

* **Terminate TLS** in a reverse proxy or set `[api] ssl_cert` and `[api] ssl_key`. Behind a proxy, start the API server with `--proxy-headers` so `X-Forwarded-*` headers are honored, and set `[api] base_url` to the public HTTPS URL.
* **Set a request-size limit and rate limits at the proxy.** The security model treats large-body memory use and authenticated DoS as the proxy's job.
* **Split the API surfaces** if workers live in a less trusted network: `airflow api-server --apps execution` serves only `/execution` for workers, and `--apps core` serves the UI and REST API for users.
* **Workers to API server over TLS**: the Task SDK trusts the system certificate store in 3.4 (`[api] client_use_public_certs = True`); `[api] client_ssl_cert` / `client_ssl_key` add a client certificate for mutual TLS.
* Keep `[api] expose_config = False`. Consider `[api] enable_swagger_ui = False` on exposed deployments.

Suggested network policy (illustrative):

| From → To | Allow? |
| :--- | :--- |
| Workers → API server `/execution` | Yes |
| Workers → metadata DB | **No** |
| Workers → broker (Celery) | Yes |
| API server → workers `8793`, triggerer `8794` | Yes (log fetch) |
| Users → API server | Via the proxy only |
| Dag processor / triggerer / scheduler / API server → metadata DB | Yes |
| Anything else → metadata DB | No |

---

## 8. Hardening Checklist

- [ ] `[core] auth_manager` is FAB or Keycloak, never Simple, in shared environments.
- [ ] `fernet_key`, `jwt_secret` / private key, and `secret_key` come from a secret store, scoped per component as in §2.
- [ ] Asymmetric JWT signing with `trusted_jwks_url` where possible.
- [ ] Workers have no `sql_alchemy_conn` and no signing key; `[celery] result_backend` uses separate credentials.
- [ ] TLS, request-size limit and rate limiting in front of the API server.
- [ ] Network policy as in §7.
- [ ] Dag changes go through code review before reaching a bundle; authors cannot write to plugins or images.
- [ ] Task-log access restricted; `[core] sensitive_var_conn_names` extended with your own secret naming.
- [ ] Fernet rotation procedure rehearsed.
- [ ] Provider and image updates tracked for security advisories.

See also: [Deployment Runbook](02_deployment_runbook.md) for which process needs which secret, and [Variables and Connections](../03_task_execution/04_xcom_variables_and_connections.md) for secrets backends.
