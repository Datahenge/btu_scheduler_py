# Architecture: Frappe Integration and the Web Server Dependency

## How we got here

The original BTU Scheduler was written in Rust. Its design goal was **complete independence from the Frappe web server**: task schedules were read directly from SQL, the Python function for each task was fetched and pickled, and the resulting RQ job was written directly into Redis. Whether gunicorn was running or not was irrelevant — the scheduler could fire jobs entirely on its own.

The Python rewrite was done quickly, under time pressure. Rather than re-implement pickling and direct RQ job construction, the scheduler was wired to call a Frappe REST API endpoint (`btu.btu_api.endpoints.enqueue_for_next_available_worker`) at the moment a task is due. Frappe handles pickling, RQ job creation, and queue placement internally. This was a deliberate shortcut that introduced a hard dependency on the Frappe gunicorn web server.

## Current architecture

The scheduler uses three integration points with Frappe:

| Layer | Technology | Web server required? |
|---|---|---|
| Read schedule definitions and cron strings | Direct SQL (asyncpg / asyncmy) | No |
| Track execution times and DST state | Direct Redis | No |
| Fire a task when it is due | Frappe REST API (HTTP POST) | **Yes** |

The first two layers can operate independently of gunicorn. The third cannot.

## What this means in practice

The scheduler knows *what* to run and *when* to run it without the web server. When a task comes due, the TSIK (Task Scheduled Instance Key) stays in the Redis sorted set until the enqueue call succeeds. If gunicorn is briefly down, the scheduler will retry on the next polling cycle rather than permanently losing the task.

However, the task does not actually run until gunicorn accepts the enqueue request. The scheduler cannot fire jobs independently — it can only hold them and retry. A sustained gunicorn outage means sustained task delay, regardless of what the SQL and Redis layers know.

This is an architectural half-measure: the scheduler carries the machinery for web-server-independent operation (SQL reads, Redis state) but cannot deliver on it at the critical moment.

## Future direction: web server first, SQL/Redis fallback

The intended improvement is a two-path enqueue strategy:

**Path A — Web server (primary):**
Call `enqueue_for_next_available_worker` as today. If the request succeeds, done.

**Path B — Direct SQL + Redis (fallback):**
If the web server is unreachable, the scheduler falls back to constructing and enqueuing the RQ job itself:
1. Read the full task definition from SQL (function path, arguments, timeout)
2. Fetch and pickle the Python function — either by calling the Frappe pickler endpoint if available, or by importing and pickling locally if the function is on the Python path
3. Write the RQ job hash directly to Redis
4. Push the job ID onto the appropriate RQ queue key

This restores the original Rust scheduler's guarantee: tasks fire on schedule regardless of gunicorn's state.

### What needs to be built

- A local pickling path that does not require the web server (the hard part — requires the task's Python module to be importable from the scheduler's environment, which may not always be true in a Frappe multi-tenant setup)
- Or: a cache of recently-pickled function bytes stored in Redis, refreshed by the web server when available, consumed by the fallback path when it is not
- Direct RQ job construction logic (previously existed in the Rust version; partially existed in the Python version as `RQJobWrapper` before being removed as dead code during the 2026 refactor)

The fallback path is non-trivial. Frappe's pickling includes site context, user context, and method dispatch that is tightly coupled to the Frappe runtime. A clean fallback may require cooperation from the BTU Frappe app to pre-cache pickled job bytes in Redis so the scheduler can consume them without an active web server.
