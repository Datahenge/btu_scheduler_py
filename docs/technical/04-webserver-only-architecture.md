# Web-Server-Only Architecture for Containerized ERPNext

**Status:** Accepted 2026-10-03. Brian confirmed the tiered recommendation below (Compose
co-location today; web-server-only as a follow-on for cross-host/managed deployments). The
`connectivity_mode` implementation landed the same day — see "Implementation status" at the
bottom of this document.

**Date:** 2026-10-03
**Branch:** version-16
**Relationship to [03-architecture-frappe-dependency.md](03-architecture-frappe-dependency.md):**
originally framed as superseding doc 03's fallback strategy; per the recommendation below, the two
are no longer treated as in conflict — see "What this means for 03-architecture-frappe-dependency.md"
in the Recommendation section.

---

## The problem this solves

BTU Scheduler currently reaches ERPNext through three channels (see
[03-architecture-frappe-dependency.md](03-architecture-frappe-dependency.md)):

| Layer | Technology | Direction |
|---|---|---|
| Read schedule definitions | Direct SQL (asyncpg / asyncmy) | BTU → DB |
| Track execution times and DST state | Direct Redis | BTU → Redis |
| Inbound control commands (cancel, reload) | Redis RPC (see [scheduler_redis_rpc.md](../scheduler_redis_rpc.md)) | Frappe → BTU |
| Fire a task when due | Frappe REST API | BTU → Frappe |

The first three all assume BTU has direct network reachability to MariaDB and/or Redis. In a
hand-rolled single-host deployment that's a non-issue. In an ERPNext stack built from Docker
Compose, it creates real operational problems:

- **Container IP instability.** Compose containers don't get stable IPs unless MariaDB and Redis
  are explicitly bound to the host (`127.0.0.1` or the host's public IP) or placed on a fixed
  user-defined network with static addressing. Without that, BTU has no reliable address to
  connect to.
- **Cross-host deployment is painful.** If BTU ever runs on a different host than the ERPNext
  stack — a reasonable thing for a client to want — direct SQL/Redis access means standing up an
  SSH tunnel (or VPN, or exposing the DB/Redis ports publicly, which is worse). Every one of
  those is extra infrastructure to build, secure, and keep alive.
- **Security surface.** Direct SQL and Redis access means two additional stateful services must
  be reachable from outside their own Compose network — either via published host ports (new
  attack surface) or a shared Docker network (coupling BTU's deployment lifecycle to ERPNext's).
  Redis in particular is risky to expose; it's protected here only by `BTU_SCHEDULER_RQ_PASSWORD`.
- **The inbound RPC channel has the same problem.** `scheduler_redis_rpc.md` chose Redis over a
  Unix Domain Socket specifically because UDS doesn't survive containerization — but Redis still
  requires TCP reachability between BTU and Frappe's Redis instance. It solves the *filesystem
  sharing* problem, not the *address stability / cross-host* problem this document is about.

## The proposed architecture

**BTU talks to ERPNext through exactly one channel: the Frappe web server, over its public DNS
name, authenticated with a User's API key/secret.** No direct SQL. No direct Redis. No RPC
channel into Redis. All four rows in the table above collapse into variations of "call a REST
endpoint."

```
BTU Scheduler  ──────HTTPS (API key/secret)──────►  https://erp.client.com
                                                      (Frappe web server)
                                                              │
                                                              ├── SQL (internal to ERPNext stack)
                                                              └── Redis (internal to ERPNext stack)
```

BTU no longer needs to know or care what IP address MariaDB or Redis have, whether they're on
the same host, or whether they moved after a `docker compose up` recreated a container. It needs
exactly one stable address: the site's public DNS name, which is already a hard requirement for
ERPNext regardless of how BTU is architected.

### What replaces each current direct-access path

| Today | Proposed replacement | New work required |
|---|---|---|
| SQL read of `tabBTU Task Schedule` | REST endpoint returning enabled schedules + cron strings | Small — a straightforward list/report endpoint on the BTU Frappe app. |
| Redis sorted set: next-execution times | Computed by BTU in-memory from the REST-fetched schedule list, same as today's `scheduler.py` logic — this part doesn't need a server-side equivalent, since it's pure computation from cron strings. | None — this logic already lives in BTU and doesn't actually require Redis itself, only the *write-your-computed-time-somewhere* step below. |
| Redis sorted set: DST fall-back dedup state (see [adr_dst_fall_back_protection.md](../adr_dst_fall_back_protection.md)) | A small persistence endpoint: "has task X already fired for local-time slot Y?" / "record that it has." | Moderate — this is real new logic on the Frappe/BTU app side. It's the one piece of state that *must* be remembered across polling cycles and currently lives in Redis for exactly that reason. |
| Redis RPC: inbound commands (cancel, reload) | BTU polls a REST endpoint for pending commands on its existing polling cycle, instead of Frappe pushing into a Redis list that BTU blocks on. | Moderate — changes the interaction from push (`BLPOP`) to pull (polling). Loses near-instant command delivery in exchange for removing the Redis dependency. |
| Frappe REST API: enqueue a due task | Unchanged — already fits this model. | None. |

### Why the latency trade-off is acceptable

The scheduler's own polling cadence (`BTU_SCHEDULER_FULL_REFRESH_INTERNAL_SECS`,
`BTU_SCHEDULER_SCHEDULER_POLLING_INTERVAL`, both defaulting to 30s) already means nothing here
is latency-sensitive at the sub-second level. Replacing a direct Redis `ZRANGE` with an HTTPS
round trip on a 30-second cycle is a non-issue. The one place latency matters more is the inbound
RPC channel, where `BLPOP` currently gives near-instant command delivery — moving that to polling
means a cancel/reload command could take up one polling interval to take effect. That seems like
an acceptable trade for a client that has explicitly prioritized operational simplicity, but it's
worth naming explicitly as a regression.

## What this costs

This is a bigger change than "stop calling SQL and Redis" — it shifts real logic and
responsibility onto the Frappe-side BTU app:

1. **New REST surface to build and maintain** on the BTU Frappe app: a schedules-list endpoint,
   a DST-dedup state endpoint, and a pending-commands endpoint. None of these exist today;
   `enqueue_for_next_available_worker` is the only precedent.
2. **Loses web-server independence entirely, by design.** The scheduler can no longer read
   schedule definitions or track any state if gunicorn is down — not even the resilience
   described in 03-architecture-frappe-dependency.md, where the scheduler "knows what to run"
   independent of the web server. This proposal accepts that trade deliberately: if the web
   server shouldn't be down, its unavailability shouldn't be a scenario BTU needs to survive
   gracefully, beyond retrying.
3. **Supersedes the current stated roadmap.** `03-architecture-frappe-dependency.md`'s "Future
   direction" section proposes the opposite: strengthening the direct-SQL/Redis fallback path so
   the scheduler survives a gunicorn outage. If this proposal is accepted, that section should be
   marked superseded rather than left as the active plan.
4. **Auth and credential management.** BTU needs a Frappe API key/secret per site instead of
   SQL/Redis credentials. Probably simpler to operate, but it's a different credential type to
   provision and rotate, and ERPNext's REST rate limiting / permission model becomes a new
   constraint to design around (the BTU API user needs the right DocType permissions, nothing
   more).

## Alternative: co-locating BTU in the same Compose project

There's a fourth option that doesn't require any new Frappe-side code: run BTU itself as a
sibling container *inside the same Docker Compose project* as ERPNext.

Compose gives every service in one project built-in DNS — containers reach each other by
**service name** (`mariadb`, `redis`), not IP, and that name resolves correctly even after a
container is recreated and picks up a new internal address. This is exactly the mechanism
Frappe's own official `frappe_docker` setup already relies on: its `scheduler` /
`queue-short` / `queue-long` worker containers are siblings in the same compose file, reaching
`mariadb` and `redis-queue` by service name, with no host port published and no IP ever
hardcoded.

So if BTU is deployed as a sibling container in the exact same Compose project as ERPNext, the
IP-instability problem that opened this document goes away for free — no host binding, no public
IP dependency, no SSH tunnel, no new code. BTU keeps using its existing direct-SQL/direct-Redis
configuration, just pointed at service names (`BTU_SCHEDULER_SQL_HOST=mariadb`,
`BTU_SCHEDULER_RQ_HOST=redis`) instead of IPs.

**What co-location does *not* fix:**

- **Different host, different Compose project, or managed/hosted ERPNext.** The moment BTU isn't
  a sibling container in the exact same project — a client's ERPNext is hosted by a third party
  (Frappe Cloud, a hosting partner's own stack) and they just want to add BTU on top — co-location
  isn't available at all, and the original SSH-tunnel problem is back. Service-name DNS only
  works within one Compose project's network.
- **Deployment lifecycle coupling.** Bundling BTU into ERPNext's own `docker-compose.yml` ties
  BTU's upgrades, restarts, and scaling to ERPNext's. This is the same coupling cost named under
  "What this costs" above for a shared Docker network — co-location doesn't remove it, it commits
  to it.
- **Credential blast radius.** BTU still holds real MariaDB and Redis credentials directly, rather
  than a scoped Frappe API key limited to specific DocType permissions. A compromised BTU
  container still has full DB/Redis reach, same as today.
- **Multi-tenant / multi-site.** A BTU instance serving more than one ERPNext site, each
  potentially in its own Compose project/network, would need to join multiple Docker networks or
  run one BTU instance per site. The web-server-only model avoids this since it only needs
  outbound HTTPS to each site's public DNS name.

In short: co-location is the container-native equivalent of "single-host deployment" — it solves
the IP-stability complaint cleanly and costs nothing to build, but only for the specific case
where BTU and ERPNext live in the same Compose project on the same host, managed by the same
operator.

## Recommendation (Claude's assessment — pending Brian's sign-off)

This is my recommendation based on the discussion above, not a decision. Treat it as settled only
once Brian confirms it.

**Ship a tiered approach instead of picking one architecture exclusively:**

1. **Today, for the common case — recommend Compose co-location, not the REST rewrite.** BTU
   already supports direct SQL/Redis configuration; it needs zero new code to run as a sibling
   container in the same Compose project as ERPNext, pointed at service names instead of IPs.
   This is the fastest path to "up and running" and matches how Frappe's own worker containers
   are deployed today. For any client where BTU and ERPNext share a host and a single Compose
   project — likely the majority of cases — this fully resolves the IP-instability problem that
   started this discussion, with no new infrastructure or endpoints to build.
2. **Treat the web-server-only architecture as the answer for the cases co-location can't cover**
   — cross-host deployment, managed/hosted ERPNext (Frappe Cloud, a hosting partner's stack), and
   multi-tenant BTU instances serving several sites. This is real, non-trivial engineering (new
   REST endpoints for schedule listing, DST-dedup state, and pending commands) and shouldn't block
   getting BTU running today. Scope it as a follow-on initiative once there's an actual client
   deployment that needs it — building it speculatively now, before a concrete cross-host case
   exists, risks guessing wrong about what the Frappe-side endpoints need to look like.
3. **Keep both modes supported long-term, not one replacing the other.** Direct SQL/Redis access
   (co-located) is simpler and requires no server-side app work; web-server-only is more portable
   and more secure. Different clients will have different constraints — don't retire the direct
   mode once the REST mode exists.

**What this means for `03-architecture-frappe-dependency.md`:** don't mark it superseded yet. Its
"fallback to direct SQL/Redis for resilience" direction and this document's "web-server-only for
portability" direction are both valid for different deployment shapes; they're not actually in
conflict once co-location is on the table as the default for single-stack deployments. I'd revisit
that document once (or if) the web-server-only work is actually scoped.

**Concrete next step for today:** add a BTU service definition to the target client's
`docker-compose.yml`, configure it with `BTU_SCHEDULER_SQL_HOST`/`BTU_SCHEDULER_RQ_HOST` set to
the MariaDB/Redis service names already in that compose file, and verify connectivity with
`btu test`. No code changes required to start running.

## Implementation status (2026-10-03, branch version-16)

Both tiers from the recommendation above are now implemented:

**btu_scheduler_py:**
- `BTU_SCHEDULER_CONNECTIVITY_MODE` (`direct` default, or `webserver`) in `lib/config.py`,
  validated so `direct` mode requires SQL/Redis settings and `webserver` mode doesn't.
- `lib/frappe_api.py` — REST replacements for the two SQL reads (schedule listing, schedule
  detail) and a new pending-commands poll.
- `lib/data_access.py` — mode-aware dispatcher so `scheduler.py` and `structs/__init__.py`
  don't need to know which backend is active.
- `lib/scheduled_store.py` — the next-execution-time sorted set and DST-fired dedup cache
  (previously always Redis) now have an in-memory implementation for `webserver` mode. This
  confirms the simplification noted during design: that state was never shared with Frappe, so
  no new Frappe-side endpoint was needed for it at all.
- `daemon/coroutines.py` — `webserver_command_poller` replaces the Redis RPC listener in
  `webserver` mode, polling the new pending-commands endpoint on the existing polling cadence.
- `Dockerfile`, `.dockerignore`, and `docker/compose.btu-scheduler.snippet.yml` +
  `docker/btu-scheduler.env.example` for the Compose co-location tier.

**BTU Frappe app** (`apps/btu`, already on `version-16`):
- New whitelisted endpoints in `btu_api/endpoints.py`: `get_enabled_task_schedules`,
  `get_task_schedule_details`, `get_pending_scheduler_commands`.
- `btu_api/scheduler.py`'s `SchedulerAPI` now checks a `btu_scheduler_connectivity_mode` site
  config key; in poll mode it pushes commands without a `response_key` and returns a synthetic
  "queued" acknowledgement instead of blocking on a `BLPOP` nothing will ever answer.
- `get_task_schedule_details` reads `cron_timezone` straight off the document rather than
  replicating the old SQL query's `tabSingles` join against `BTU Configuration.cron_time_zone`
  — that field no longer exists on the DocType; `BTUTaskSchedule.before_validate()` already
  backfills `cron_timezone` from the system time zone at save time, so the join was stale.

**Not yet done:**
- No `bench run-tests` pass against a live site — the Frappe-side endpoints are syntax-checked
  only. Run `bench --site <site> migrate` (no schema changes, but safe) and exercise
  `btu test` / the BTU Configuration "Send Ping" and "Resubmit All" buttons against a real site
  before relying on `webserver` mode in production.
- The Docker image hasn't been build-tested (no Docker daemon available in the session that
  wrote it) — build it once before deploying.

## Open questions still worth Brian's input

- Is losing inbound command responsiveness (RPC push → polling) acceptable for the eventual
  web-server-only mode, or does the RPC channel need to stay as an exception even then?
- Should the DST-dedup state (when the web-server-only work is eventually scoped) live in Frappe's
  SQL database or behind a thin Redis wrapper?
- Is there already a concrete client/deployment on the horizon that needs cross-host or
  multi-tenant support, which would justify scoping the web-server-only work now rather than
  later?
