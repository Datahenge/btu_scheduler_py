# BTU Scheduler Python — Multi-Phase Refactor Plan

## Context
The project was written quickly by porting the Rust scheduler, resulting in vestigial Rust artifacts, dead code, inconsistent type annotations, silent exception handling, and a few latent bugs. The goal is a clean, idiomatic Python 3.11+ codebase. This plan is sequenced so each phase stands alone — phases can be committed and shipped independently.

---

## Phase 1 — Dead Code Removal *(complete)*

**Why first:** Removing dead code before anything else narrows the codebase to what actually runs, so every subsequent phase has less surface. Zero behavioral risk.

**Items deleted:**

`btu_rq.py`:
- `RQJobWrapper.DEL_create_only()` — unused; also contained `self.self` latent bug
- `RQJobWrapper.DEL_create_and_enqueue()` — empty stub
- `DEL_enqueue_job_immediate()` — zero callers
- `create_raw_connection()` — zero callers; use `create_connection(decode_responses=False)` directly

`sql.py`:
- `get_enabled_tasks()` — zero callers (vs `get_enabled_task_schedules()` which is active)

`utils.py`:
- `validate_datatype()` — zero callers
- `whatis()` — copy-pasted from an unrelated project (`dw_etl.generics`), never adapted
- `DictToDot` class — zero callers or importers
- `utc_to_rq_string()` — zero callers
- `import inspect` — only used by `whatis()`; removed with it
- Top-level `from slack_sdk.webhook import WebhookClient` — moved inside `send_message_to_slack()`

`structs/__init__.py`:
- `BtuTask.convert_to_wrapped_rq_job()` — dead; also called `await`-less async function (latent coroutine-object bug)
- `BtuTaskSchedule.to_rq_job_wrapper()` — dead; old pre-HTTP design
- Orphaned imports of `RQJobWrapper` and `get_pickled_function_from_web`

`diagnostics.py`:
- `decode_redis()` — recursive helper with no external callers

`cli/__init__.py`:
- `VERBOSE_MODE` global — set but never read; removed flag and `--verbose` option
- Incomplete second `subprocess.run` in `service-status` command

`scheduler.py`:
- Three Rust `static` constant comments

---

## Phase 2 — Bug Fixes and Deprecated API Calls

**Why second:** These are real correctness issues. Fix them before the refactor obscures them.

- `raise ex` → bare `raise` in two places:
  - `daemon/__init__.py` line 95
  - `daemon/coroutines.py` line 393
- `asyncio.get_event_loop()` → `asyncio.get_running_loop()` in `coroutines.py` line 451
- `zrangebyscore` → `ZRANGE ... BYSCORE` in `scheduler.py` (deprecated since Redis 6.2.0)
- `uuid_string: str = uuid.uuid4()` → `uuid_obj: uuid.UUID = uuid.uuid4()` in `btu_rq.py` (wrong annotation)
- `rq_cancel_scheduled_task() -> tuple` but returns `None` — fix annotation in `scheduler.py`
- `RQScheduledTask.from_tsik() -> object` → `-> RQScheduledTask` in `scheduler.py`

---

## Phase 3 — Type Annotation Modernization

**Why third:** With dead code gone and bugs fixed, clean annotations are a stable target.

- Remove `from __future__ import annotations` from `btu_cron.py` and `btu_rq.py` — minimum Python is 3.11; not needed
- Replace `Union[X, NoneType]` / `NoneType = type(None)` with `X | None` throughout:
  - `btu_rq.py`, `structs/__init__.py`, `structs/sanchez.py`
  - Remove the `NoneType = type(None)` module-level alias from all four files
- Remove `from typing import Union` once all usages are gone
- Annotate the unannotated `async def` in `daemon/coroutines.py` and `daemon/__init__.py`
- Change `internal_queue` parameter type from `object` to `asyncio.Queue[str]` throughout

---

## Phase 4 — Code Hygiene

**Why fourth:** Mechanical cleanups that make the code readable before structural changes.

- Remove all commented-out code blocks:
  - `scheduler.py` (several commented-out log/print calls)
  - `coroutines.py` (commented-out log calls)
  - `diagnostics.py` (commented-out print calls)
- Replace `print()` in `rq_print_scheduled_tasks()` (`scheduler.py`) with `log.info()` or return a formatted string
- Fix `except Exception: pass` silent swallowing in `coroutines.py` — at minimum log at `DEBUG` level with the exception message
- Replace `ssl._create_unverified_context()` (private API) in `utils.py` and `diagnostics.py` with the public `ssl.create_default_context()` + explicit comment explaining why verification is disabled
- Fix typo: `couroutines` → `coroutines` in `coroutines.py` module docstring and `internal_queue_consumer()` docstring
- Remove redundant deferred `from btu_scheduler.lib import scheduler` import inside `_dispatch_redis_command()` in `coroutines.py` — already imported at module level
- Replace "vector" terminology in comments with "list" throughout `scheduler.py` and `btu_rq.py`

---

## Phase 5 — Structural Refactoring

**Why fifth:** These touch multiple files and should happen after the codebase is clean and annotated.

### 5a. Extract `_build_frappe_headers(config)` helper

Four call sites duplicate the same `Authorization` + `Content-Type` + optional `Host` header construction:
- `structs/__init__.py`
- `structs/sanchez.py`
- `diagnostics.py` (two call sites)

Extract to `lib/utils.py` (or a new `lib/http_utils.py`) and call from all four sites.

### 5b. Unify TCP and Redis command dispatch

`handle_tcp_request()` (`coroutines.py`, ~230 lines) and `_dispatch_redis_command()` implement the same logical commands. Extract a pure `_execute_command(command_name, payload, config) -> dict` function. Both paths call it after their respective transport parsing.

### 5c. Evaluate TSIK elimination

`TSIK` is a thin string-wrapper newtype that exists because Rust requires distinct types; Python does not. Consider whether `RQScheduledTask` can parse the composite `"{id}|{timestamp}"` string directly, eliminating the `TSIK → RQScheduledTask` conversion chain. Do this last, after tests exist.

---

## Phase 6 — Test Coverage

Current state: one test file covering only `tz_cron_to_utc_datetimes()`.

Priority additions (no running services required):
1. `scheduler.py` — `TSIK` parsing/formatting, `RQScheduledTask` helpers
2. `btu_rq.py` — `RQJobWrapper` field extraction from mock Redis hash data
3. `structs/__init__.py` — `BtuTaskSchedule` and `BtuTask` dataclass construction
4. `cli/__init__.py` — Click command smoke tests using `click.testing.CliRunner`

---

## Critical Files

| File | Phases |
|---|---|
| `src/btu_scheduler/lib/btu_rq.py` | 1 ✓, 2, 3 |
| `src/btu_scheduler/lib/utils.py` | 1 ✓, 4, 5a |
| `src/btu_scheduler/lib/structs/__init__.py` | 1 ✓, 3, 5a |
| `src/btu_scheduler/lib/scheduler.py` | 1 ✓, 2, 4, 5b, 5c |
| `src/btu_scheduler/daemon/coroutines.py` | 2, 3, 4, 5b |
| `src/btu_scheduler/lib/diagnostics.py` | 1 ✓, 4, 5a |
| `src/btu_scheduler/lib/structs/sanchez.py` | 3, 5a |
| `src/btu_scheduler/cli/__init__.py` | 1 ✓, 2 |

---

## Verification

After each phase:
1. `python -m pytest src/btu_scheduler/tests/` — no regressions on cron tests
2. `ruff check src/` — clean
3. `ruff format --check src/` — clean
4. Manual smoke test: `btu config show`, `btu test all` (covers Redis + SQL + Frappe connectivity)

After Phase 5: end-to-end daemon run with `btu run-daemon` in a dev environment with a real Frappe instance.
