# ADR: DST Fall-Back Double-Fire Protection

**Status:** Accepted  
**Date:** 2026-07-05  
**Branch:** version-15

---

## Context

BTU Scheduler is a stateless daemon.  Its core scheduling loop works like this:

1. Query SQL for all enabled Task Schedules.
2. For each schedule, calculate its next UTC execution time from the current moment.
3. Write that time into a Redis sorted set (`btu_scheduler:task_execution_times`).
4. Periodically scan the sorted set for entries whose time has arrived, and fire them.
5. After a job fires, discard it from the sorted set and re-queue the Task Schedule ID
   for the next recalculation cycle (back to step 2).

This design is one of BTU Scheduler's greatest strengths.  Because every next-execution
time is recalculated fresh from the current moment and authoritative sources (SQL + Redis),
the scheduler is resilient: crashes, restarts, and mid-flight configuration changes are all
handled gracefully with no stale state to reconcile.

However, this same design has one structural weakness:

> **The scheduler asks "what comes next from now?" but has no memory of "what already fired."**

Under normal conditions those two questions have the same answer.  Daylight Saving Time
fall-back is the one case per year where they diverge.

---

## The Problem: DST Fall-Back

On the night clocks fall back (e.g., 2026-11-01 in North America), the clock reads
`1:00 AM EDT` at `05:00 UTC`, then at `06:00 UTC` it falls back to `1:00 AM EST`.
The entire `1:00–1:59 AM` window occurs **twice** on that calendar day.

For a Task Schedule with cron `"30 1 * * *"` (1:30 AM daily):

| UTC   | Local (Eastern)  | What happens                                  |
|-------|------------------|-----------------------------------------------|
| 05:30 | 1:30 AM EDT      | Job fires. Scheduler recalculates from ~05:31 UTC. |
| 05:31 | (recalculation)  | Next `"1:30 AM"` in local time = **1:30 AM EST = 06:30 UTC**. Scheduled. |
| 06:30 | 1:30 AM EST      | Job fires again. **Double-fire.** |

The recalculation at 05:31 UTC is not wrong in isolation — it correctly answers
"when does this cron next fire?".  The problem is that it has no awareness of the
firing that just happened.

---

## The Problem: DST Spring-Forward

On the night clocks spring forward (e.g., 2026-03-08), the clock jumps from
`2:00 AM EST` to `3:00 AM EDT` at `07:00 UTC`.  The entire `2:00–2:59 AM` window
**does not exist** on that calendar day.

For a cron `"30 2 * * *"` (2:30 AM daily), we initially expected croniter to
skip to the next day (2:30 AM EDT on 2026-03-09).  But the correct behavior is
actually to fire at the **transition point** — 3:00 AM EDT on the same day.

**Reasoning:** if a business process is scheduled at 2:30 AM and that time does not
exist, firing 30 minutes late is far less surprising than firing 24 hours late.
A Purchase Order that arrives at 3:00 AM is a minor anomaly; one that arrives a full
day late is a business failure.  croniter 6.2.3 implements this correctly.

---

## croniter History

Understanding how we got here requires a brief history of the `croniter` library's
DST handling.

| Version | Date | Event |
|---|---|---|
| 0.3.5 and earlier | ~2014 | DST completely broken; behavior depended on system timezone (Issue #35) |
| 0.3.16 | 2017-03-15 | First real DST support attempt |
| 0.3.18–0.3.20 | 2017 | Multiple DST fix iterations |
| 0.3.32 | 2020-05-27 | Additional DST boundary tests and fixes |
| — | 2020-10 | Issue #147 opened: wrong datetimes still produced at fall-back transitions |
| 6.0.0 | 2024-12-17 | Project transferred to `pallets-eco`; timestamp rework, no DST logic change |
| **6.1.0** | **2026-03-14** | **"Fix DST handling by rewriting the DST logic."** — Benjamin Drung |
| 6.2.3 | 2026-07-02 | Latest stable as of this ADR |

BTU Scheduler had pinned `croniter==6.0.0`.  Two spring-forward tests were failing —
confirmed pre-existing, unrelated to any BTU change.  After upgrading to 6.2.3, the
failures changed in character (spring-forward failure mode shifted; a fall-back test
newly failed), which prompted the deeper analysis documented here.

The test expectations for spring-forward were incorrect (they expected
"skip to next day" behavior).  The test for fall-back double-firing was correct.

---

## Approaches Considered

### Approach 1: Request N > 1 results at scheduling time

If the scheduler requested N=2 (or more) results at once, it could inspect
consecutive pairs for fall-back duplicates before writing to Redis.

**Rejected as insufficient.**  This helps at *initial scheduling time*, but not at
*re-queue time*.  After the first fall-back occurrence fires and the scheduler
recalculates from `~05:31 UTC`, only the second occurrence (`06:30 UTC`) is visible —
the first is in the past.  With no consecutive pair to inspect, no deduplication occurs.
The double-fire still happens.

### Approach 2: 2-hour lookahead window

Schedule all occurrences that fall within the next 2 hours.  A 2-hour window is
justified by DST physics — no transition offsets more than 1 hour — so both sides of
any transition are always visible together.

This approach was briefly considered compelling.

**Rejected as insufficient**, for the same reason as Approach 1.  The
user identified the flaw precisely:

> *"At 1:25 AM, we'll have a great set of future results and can deduplicate.
> But what happens after DST?  The 2-hour lookahead won't contain duplicates
> anymore.  So the scheduler will just fire them again."*

After the first occurrence fires and the scheduler recalculates, the lookahead window
starts fresh from the new "now".  The second fall-back occurrence is the only result
in the window.  No duplicate is visible.  It fires.

### Approach 3: Historical UTC execution cache

Cache the UTC timestamps of recently-executed jobs.  Before firing, check if that
exact UTC timestamp was already executed.

**Rejected as solving the wrong problem.**  The fall-back double-fire involves two
*different* UTC timestamps: `05:30 UTC` and `06:30 UTC`.  A UTC-keyed cache would see
two distinct keys and permit both firings.  It does not address the core issue.

A UTC cache is useful for a different scenario (e.g., accidental re-queuing of an
already-fired slot), but it is not the DST solution.

### Approach 4: Local-slot execution cache (selected)

Cache the *local time slot* — `(task_schedule_id, date, hour, minute)` in the
schedule's own `cron_timezone` — for recently-executed jobs.  Before firing, check
if that local slot already executed today.

**This is the correct key.**  Both `05:30 UTC` (1:30 AM EDT) and `06:30 UTC`
(1:30 AM EST) map to the same local slot: `2026-11-01 01:30`.  The cache correctly
identifies them as the same calendar event regardless of their different UTC values.

Redis is already present in the architecture.  A key with a 25-hour TTL is
lightweight, survives scheduler restarts, and requires no schema changes.

---

## The Frequency Distinction

A critical subtlety: the deduplication rule must account for the cron's natural interval.

| Cron | Interval | Fall-back behavior |
|---|---|---|
| `"30 1 * * *"` | 24 hours | Should fire **once** — it is a daily slot |
| `"30 * * * *"` | 1 hour | Should fire **once** — it is an hourly slot |
| `"*/15 * * * *"` | 15 minutes | Should fire **twice** — both 1:30 AM EDT and 1:30 AM EST are distinct 15-minute slots |

The local-slot deduplication rule handles this correctly **without needing to know the
interval** because it operates on consecutive results from the iterator:

- For a daily cron, consecutive results are: `[1:30 AM EDT, 1:30 AM EST]` — same local
  slot → second is dropped.
- For a 15-minute cron, consecutive results are: `[..., 1:30 EDT, 1:45 EDT, ...]` then
  `[..., 1:00 EST, 1:15 EST, 1:30 EST, ...]`.  No two *consecutive* results share the
  same local `(date, hour, minute)`, so nothing is dropped.

The 1:30 AM EDT and 1:30 AM EST occurrences for the 15-minute cron are separated by
three other results.  They are never consecutive.  They both fire.  This is correct.

---

## The Architectural Insight

The recalculation model is a feature, not a bug.  It should not be replaced with a
stateful tracking system.  Instead, it needs the minimum possible addition of historical
awareness to handle the one case per year where "what comes next" and "what should
actually execute" diverge.

> The scheduler's greatest strength — stateless recalculation — is also, precisely once
> per year, a weakness: recalculating "what needs to happen" without knowledge of what
> already happened.

The local-slot cache is the surgical answer.  It does not make the scheduler generally
stateful.  It adds exactly one piece of historical memory:

> *"Did this Task Schedule already fire for this local (date, hour, minute) today?"*

---

## Decision: Defense in Depth

Two complementary layers were implemented.

### Layer 1 — Function-level dedup (`btu_cron.py`)

`tz_cron_to_utc_datetimes` now inspects consecutive results and skips any that share
the same local `(date, hour, minute)` as the preceding result.  This prevents
fall-back duplicates from appearing in result sets when `N >= 2` results are requested.

This layer helps at scheduling time and provides correct behavior for callers that
request multiple results (e.g., future tooling, diagnostics).

> **Note:** As of this writing, the scheduler's normal path calls `get_next_runtimes()`
> with no arguments, which defaults to `number_results=1`.  With only one result to
> fill, the consecutive-pair comparison never has a "previous" to check against —
> Layer 1 is **currently dormant in production**.  Layer 2 (the Redis cache) is the
> sole active DST protection in the live scheduling path.  Layer 1 will become active
> if any caller requests `number_results >= 2`, such as a future diagnostic command or
> pre-scheduling lookahead.

### Layer 2 — Local-slot execution cache (`scheduler.py`)

`run_immediate_scheduled_task` checks a Redis key before every enqueue:

```
btu:fired:{task_schedule_id}:{YYYY-MM-DD}:{HH}:{MM}    (TTL: 90,000 seconds / 25 hours)
```

The key is derived from the UTC execution time converted to the schedule's own
`cron_timezone`.  If the key exists, the enqueue is suppressed and a warning is logged.
The slot is still removed from the sorted set and the task is re-queued for its next
legitimate execution time.  If the key does not exist, execution proceeds normally and
the key is written immediately after a successful enqueue.

This layer is authoritative.  It handles the re-queue path, where Layer 1 cannot
operate because only one result is visible at a time.  It also provides protection
across scheduler restarts that occur mid-transition.

---

## Consequences

- **Fall-back:** Daily and hourly jobs fire exactly once on fall-back night.  Sub-hourly
  jobs (interval < 1 hour) correctly fire at every matching slot, including both
  occurrences of the ambiguous hour.
- **Spring-forward:** Jobs scheduled during the gap fire at the transition point
  (3:00 AM on spring-forward day), not 24 hours later.
- **Redis key overhead:** One key per task schedule per calendar day, 25-hour TTL.
  On a system with 100 task schedules, this is 100 keys.  Negligible.
- **Restart safety:** The Redis cache survives scheduler restarts within the 25-hour
  window, preventing a crash-and-restart during fall-back from re-firing a completed slot.
- **Observability:** Suppressed fall-back firings emit a `WARNING` log entry identifying
  the task schedule and local slot, making the behavior visible in logs.
