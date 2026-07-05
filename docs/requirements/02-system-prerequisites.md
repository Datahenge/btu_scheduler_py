# System Prerequisites

External services and runtime dependencies required to run BTU Scheduler.

## Python

**Minimum: 3.11**

BTU Scheduler uses `asyncio.TaskGroup` (3.11+), the `X | None` union syntax (3.10+), and `zoneinfo` (3.9+). Earlier Python versions are not supported.

## Redis Server

**Minimum: 6.2.0**

> This is stricter than Frappe's stated requirement of "Redis 6."

Frappe Framework v15 documents its Redis requirement as version 6, but BTU Scheduler uses the `ZRANGE ... BYSCORE` command (`redis-py`: `zrange(..., byscore=True)`), which was added to the Redis server in **6.2.0** (released February 2021). Redis 6.0.x and 6.1.x do not implement this command and will return an `ERR unknown subcommand` error at runtime when the scheduler attempts to fetch due task schedules.

**Why not revert to `zrangebyscore`?**  
`ZRANGEBYSCORE` was deprecated by Redis in 6.2.0 in favour of `ZRANGE BYSCORE`. Keeping the deprecated form would silence the deprecation concern while silently allowing deployment on Redis versions that are themselves five or more years old by 2026. The 6.2.0 floor is a reasonable minimum.

**Verify your Redis version:**
```bash
redis-cli INFO server | grep redis_version
```

## Python Redis Client (`redis-py`)

BTU Scheduler pins `redis==5.2.1`. Frappe v15 uses `redis~=4.5.5` in its own virtualenv. These are independent processes sharing the same Redis server — the two `redis-py` versions do not conflict. Both 4.5.x and 5.x support `zrange(..., byscore=True)`.

## Database

MariaDB or PostgreSQL, whichever Frappe is configured to use. BTU Scheduler connects read-only to the Frappe database to fetch BTU Task Schedule records. The connection is governed by the `BTU_SCHEDULER_SQL_*` environment variables.

See [01-environment-variables.md](01-environment-variables.md) for the full variable reference.

## Frappe Framework

Version 15 (`version-15` branch). The scheduler enqueues jobs by calling Frappe's REST API endpoint `btu.btu_api.endpoints.enqueue_for_next_available_worker`. The BTU app must be installed in the Frappe site and the web server must be reachable from the host running BTU Scheduler.
